package com.kevincheng.papercupphone

import android.app.Service
import android.content.Intent
import android.os.*
import android.system.ErrnoException
import android.system.OsConstants
import android.util.Log
import android.widget.Toast
import com.kevincheng.papercupphone.paho.MqttAndroidClientExtended
import com.orhanobut.logger.Logger
import org.eclipse.paho.client.mqttv3.*
import org.greenrobot.eventbus.EventBus
import org.greenrobot.eventbus.Subscribe
import org.greenrobot.eventbus.ThreadMode
import org.json.JSONException
import org.json.JSONObject
import java.io.Serializable
import java.lang.ref.WeakReference
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.collections.ArrayList
import org.eclipse.paho.client.mqttv3.MqttMessage

class PaperCupPhone : Service() {
    companion object {
        val TAG = PaperCupPhone::class.java.simpleName
        var isRunning: Boolean = false
    }

    private lateinit var mBackgroundThread: HandlerThread
    private lateinit var mBackgroundHandler: Handler
    private lateinit var mCommunicationThread: HandlerThread
    private lateinit var mCommunicationHandler: Handler
    private lateinit var mBrokerURI: String
    private lateinit var mMQTTAndroidClient: MqttAndroidClientExtended
    private lateinit var mMQTTConnectOptions: MqttConnectOptions
    private lateinit var mClientId: String
    private lateinit var mCallback: MQTTCallback
    private lateinit var mMQTTConnectionListener: MQTTConnectionListener
    private lateinit var mInitializeSubscriptionTopic: Array<String>
    private lateinit var mInitializeSubscriptionQoS: IntArray
    private lateinit var mSubscriptionTopic: Array<String>
    private lateinit var mSubscriptionQoS: IntArray
    private lateinit var mCachedSubscriptionTopic: Array<String>
    private lateinit var mCachedSubscriptionQoS: IntArray
    private lateinit var mConnectToBrokerRunnable: ConnectToBrokerRunnable

    private var isAutomaticReconnect: Boolean = false
    private var isCleanSession: Boolean = true
    private var keepAliveInterval: Int = 60
    private var retryInterval: Int = 15
    private var isConnectionCompletedOnce: Boolean = false
    private var isDestroyed: Boolean = false
    private var didSetupDisconnectedBufferOptions: Boolean = false

    override fun onCreate() {
        isRunning = true
        Toast.makeText(applicationContext, "Start MQTT Service", Toast.LENGTH_SHORT).show()
        mBackgroundThread = HandlerThread("PaperCupPhone-BackgroundThread", Process.THREAD_PRIORITY_BACKGROUND)
        mBackgroundThread.start()
        mBackgroundHandler = Handler(mBackgroundThread.looper)
        mCommunicationThread = HandlerThread("PaperCupPhone-CommunicationThread", Process.THREAD_PRIORITY_BACKGROUND)
        mCommunicationThread.start()
        mCommunicationHandler = Handler(mCommunicationThread.looper)
        EventBus.getDefault().register(this@PaperCupPhone)
    }

    override fun onBind(intent: Intent?): IBinder? {
        return null
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent == null) throw NoSuchElementException()
        val launcher = intent.getSerializableExtra(Launcher.name) as? Launcher
                ?: throw NoSuchElementException()

        mClientId = MqttClient.generateClientId()
        mBrokerURI = launcher.brokerURI
        mInitializeSubscriptionTopic = launcher.topic + "${launcher.topicPrefix}$mClientId/"
        mInitializeSubscriptionQoS = launcher.qos + 1
        mSubscriptionTopic = arrayOf()
        mSubscriptionQoS = IntArray(0)
        mCachedSubscriptionTopic = arrayOf()
        mCachedSubscriptionQoS = IntArray(0)
        isAutomaticReconnect = launcher.isAutomaticReconnect
        isCleanSession = launcher.isCleanSession
        keepAliveInterval = launcher.keepAliveInterval
        retryInterval = launcher.retryInterval
        mMQTTConnectionListener = MQTTConnectionListener(WeakReference(this@PaperCupPhone))

        // Config Mqtt Client
        mMQTTAndroidClient = MqttAndroidClientExtended(applicationContext, mBrokerURI, mClientId)
        mCallback = MQTTCallback(false, isCleanSession, WeakReference(this@PaperCupPhone), WeakReference(mMQTTAndroidClient), mClientId)
        mMQTTAndroidClient.setCallback(mCallback)
        mMQTTConnectOptions = MqttConnectOptions()
        mMQTTConnectOptions.isAutomaticReconnect = isAutomaticReconnect
        mMQTTConnectOptions.isCleanSession = isCleanSession
        mMQTTConnectOptions.keepAliveInterval = keepAliveInterval

        mConnectToBrokerRunnable = ConnectToBrokerRunnable(WeakReference(this@PaperCupPhone))
        mBackgroundHandler.post(mConnectToBrokerRunnable)

        Logger.i("Service Has Started\nClient Id: $mClientId")

        return Service.START_NOT_STICKY
    }

    override fun onDestroy() {
        isRunning = false
        Toast.makeText(applicationContext, "Stop MQTT Service", Toast.LENGTH_SHORT).show()
        EventBus.getDefault().unregister(this@PaperCupPhone)
        mBackgroundHandler.removeCallbacksAndMessages(null)
        mBackgroundThread.quit()
        mCommunicationHandler.removeCallbacksAndMessages(null)
        mCommunicationThread.quit()

        mMQTTAndroidClient.disconnectImmediately()
        super.onDestroy()
        isDestroyed = true
        Logger.i("Service Has Been Destroyed\nClient Id: $mClientId")
        sendOutConnectionStatus(false) // Notify that the connection has been disconnected
    }

    private fun connectToBroker() {
        try {
            Logger.i("Start Connecting To The Broker: $mBrokerURI, Client Id: $mClientId")
            mMQTTAndroidClient.connect(mMQTTConnectOptions, null, mMQTTConnectionListener)
        } catch (ex: Exception) {
            when (ex) {
                is NullPointerException -> Logger.v("Service Has Been Destroyed And The Above Operations Will Be Cancelled")
                else -> {
                    Logger.e(ex, "throwable")
                    Toast.makeText(applicationContext, "Unknown Error Occurred", Toast.LENGTH_LONG).show()
                    stopSelf()
                }
            }
        }
    }

    private fun reconnectToBroker() {
        val interval: Long = (retryInterval * 1000).toLong()
        Logger.v("Retry The Connection After $interval Milliseconds")
        mBackgroundHandler.postDelayed(mConnectToBrokerRunnable, interval)
    }

    private fun initializeSubscription() {
        subscribeTopic(mInitializeSubscriptionTopic, mInitializeSubscriptionQoS, InitializeSubscriptionListener(WeakReference(this@PaperCupPhone), CountDownLatch(1)))
    }

    private fun subscribeTopic(topic: Array<String>, qos: IntArray, listener: IMqttActionListener?) {
        var nullableListener: IMqttActionListener? = null
        var gate: CountDownLatch = CountDownLatch(1)
        when (listener) {
            is InitializeSubscriptionListener -> {
                nullableListener = listener
                gate = listener.gate
            }
            is SubscriptionListener -> {
                nullableListener = listener
                gate = listener.gate
            }
            null -> nullableListener = SubscriptionListener(WeakReference(this@PaperCupPhone), topic, qos, gate)
        }

        try {
            mMQTTAndroidClient.subscribe(topic, qos, null, nullableListener)
            gate.await()
        } catch (ex: Exception) {
            when (ex) {
                is NullPointerException -> Logger.v("Service Has Been Destroyed And The Above Operations Will Be Cancelled")
                else -> {
                    when (ex) {
                        is IllegalArgumentException -> Logger.e(ex, "Two Supplied Arrays Are Not The Same Size")
                        is MqttException -> Logger.e(ex, "An Error Registering The Subscription.")
                        else -> Logger.e(ex, "throwable")
                    }
                    Toast.makeText(applicationContext, "Unknown Error Occurred", Toast.LENGTH_LONG).show()
                    stopSelf()
                }
            }
        }
    }

    private fun sendOutConnectionStatus() {
        val status = when (isConnectionCompletedOnce) {
            true -> try {
                mMQTTAndroidClient.isConnected
            } catch (ex: Exception) {
                false
            }
            else -> isConnectionCompletedOnce
        }
        sendOutConnectionStatus(status)
    }

    private fun sendOutConnectionStatus(status: Boolean) {
        val jsonObject = JSONObject()
        jsonObject.put("action", "connectionStatus")
        jsonObject.put("status", status)
        EventBus.getDefault().post(Event.IncomingMessage(jsonObject))
    }

    private fun setupOfflinePublishingMessageBuffer() {
        if (didSetupDisconnectedBufferOptions) return
        try {
            val disconnectedBufferOptions = DisconnectedBufferOptions()
            disconnectedBufferOptions.isBufferEnabled = true
            disconnectedBufferOptions.bufferSize = 500
            disconnectedBufferOptions.isPersistBuffer = false
            disconnectedBufferOptions.isDeleteOldestMessages = true
            mMQTTAndroidClient.setBufferOpts(disconnectedBufferOptions)
            didSetupDisconnectedBufferOptions = true
        } catch (ex: Exception) {
            when (ex) {
                is NullPointerException -> Logger.v("Service Has Been Destroyed And The Above Operations Will Be Cancelled")
                else -> Logger.e(ex, "throwable")
            }
        }
    }

    @Subscribe(threadMode = ThreadMode.BACKGROUND)
    fun onMessageEvent(event: PaperCupPhone.Event.Topic.Subscribe) {
        mCommunicationHandler.post(SubscriptionRunnable(WeakReference(this@PaperCupPhone), event, retryInterval))
    }

    @Subscribe(threadMode = ThreadMode.BACKGROUND)
    fun onMessageEvent(event: PaperCupPhone.Event.Topic.Unsubscribe) {
        mCommunicationHandler.post(UnsubscribeRunnable(WeakReference(this@PaperCupPhone), event, retryInterval))
    }

    @Subscribe(threadMode = ThreadMode.BACKGROUND)
    fun onMessageEvent(event: PaperCupPhone.Event.Topic.PublishMessage) {
        mCommunicationHandler.post(PublishMessageRunnable(WeakReference(this@PaperCupPhone), event, retryInterval))
    }

    @Subscribe(threadMode = ThreadMode.MAIN)
    fun onMessageEvent(event: PaperCupPhone.Event.GetConnectionStatus) {
        sendOutConnectionStatus()
    }

    private class MQTTConnectionListener(val weakSelf: WeakReference<PaperCupPhone>) : IMqttActionListener {
        override fun onSuccess(asyncActionToken: IMqttToken) {
            val self = weakSelf.get() ?: return
            Toast.makeText(self.applicationContext, "MQTT Connection Succeeded", Toast.LENGTH_SHORT).show()
            Logger.d("Connection[${self.mClientId}] Succeeded Between ${self.mBrokerURI}")
        }

        override fun onFailure(asyncActionToken: IMqttToken, exception: Throwable) {
            val self = weakSelf.get() ?: return
            Toast.makeText(self.applicationContext, "MQTT Connection Failed", Toast.LENGTH_SHORT).show()
            Logger.w("Connection[${self.mClientId}] Failed Between ${self.mBrokerURI}")
            var hasBeenHandled = true
            val expectedException = exception.cause?.cause
            if (expectedException != null) {
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.LOLLIPOP) {
                    val errnoException = expectedException as? ErrnoException
                    if (errnoException != null) {
                        when (errnoException.errno) {
                            OsConstants.ENETUNREACH -> Logger.w("There Is No Internet Connection, But It Should Automatically Reconnect When The Internet Is Available")
                            else -> {
                                when (errnoException.errno) {
                                    OsConstants.ECONNREFUSED -> Logger.w("The Host Is Online But Connection to Broker Failed")
                                    OsConstants.EHOSTUNREACH -> Logger.w("The Host Is Offline")
                                    else -> hasBeenHandled = false
                                }
                                self.reconnectToBroker()
                            }
                        }
                    } else {
                        hasBeenHandled = false
                    }
                } else {
                    val message = expectedException.message
                    if (message != null) {
                        when {
                            message.contains("ENETUNREACH") -> Logger.w("There Is No Internet Connection, But It Should Automatically Reconnect When The Internet Is Available")
                            else -> {
                                when {
                                    message.contains("ECONNREFUSED") -> Logger.w("The Host Is Online But Connection to Broker Failed")
                                    message.contains("EHOSTUNREACH") -> Logger.w("The Host Is Offline")
                                    else -> hasBeenHandled = false
                                }
                                self.reconnectToBroker()
                            }
                        }
                    } else {
                        hasBeenHandled = false
                    }
                }
            } else {
                hasBeenHandled = false
            }
            if (!hasBeenHandled) Logger.e(exception, "Unexpected Throwable")
        }
    }

    private class MQTTCallback(var isInitialized: Boolean, val isCleanSession: Boolean, val weakSelf: WeakReference<PaperCupPhone>, val weakClient: WeakReference<MqttAndroidClientExtended>, val clientId: String) : MqttCallbackExtended {
        override fun connectComplete(reconnect: Boolean, serverURI: String?) {
            Logger.d("Connection[$clientId] Completed")
            val self = weakSelf.get()
            if (self != null && !self.isDestroyed) {
                // Message buffer of publish message when offline
                self.setupOfflinePublishingMessageBuffer()
                self.isConnectionCompletedOnce = true
                self.sendOutConnectionStatus()
                self.mBackgroundHandler.post {
                    val self = weakSelf.get() ?: return@post
                    when (reconnect) {
                        true -> {
                            if (!isInitialized) {
                                self.initializeSubscription()
                                return@post
                            }
                            if (!isCleanSession) return@post
                            // If Clean Session is true, we need to re-subscribe
                            self.subscribeTopic(self.mCachedSubscriptionTopic, self.mCachedSubscriptionQoS, null)
                        }
                        false -> self.initializeSubscription()
                    }
                }
            } else {
                Logger.w("Connection[$clientId] Completed But It Should Disconnected Immediately Because The Service Has Been Destroyed")
                when {
                    self == null -> {
                        val client = weakClient.get() ?: return
                        client.disconnectImmediately()
                    }
                    self.isDestroyed -> self.mMQTTAndroidClient.disconnectImmediately()
                }
            }
        }

        override fun connectionLost(cause: Throwable?) {
            Logger.w("Connection[$clientId] Lost")
            val self = weakSelf.get() ?: return
            if (self.isDestroyed) return
            self.sendOutConnectionStatus()
            if (!isCleanSession) return
            self.mBackgroundHandler.post {
                val self = weakSelf.get() ?: return@post
                val tempSubscriptionTopic = self.mSubscriptionTopic
                val tempSubscriptionQoS = self.mSubscriptionQoS
                self.mSubscriptionTopic = arrayOf()
                self.mSubscriptionQoS = IntArray(0)
                tempSubscriptionTopic.forEach { self.mCachedSubscriptionTopic += it }
                tempSubscriptionQoS.forEach { self.mCachedSubscriptionQoS += it }
            }
        }

        override fun messageArrived(topic: String?, message: MqttMessage?) {
            Log.d(TAG, "Connection[$clientId] Received Message: <topic: $topic, message: $message>")
            val self = weakSelf.get() ?: return
            if (message != null && !self.isDestroyed) {
                try {
                    val jsonObject = JSONObject(message.toString())
                    EventBus.getDefault().post(Event.IncomingMessage(jsonObject))
                } catch (e: JSONException) {
                    Logger.e(e, "throwable", message)
                }
            } else {
                when {
                    self.isDestroyed -> Logger.i("Connection[$clientId] Received Message But The Message Should Not Be Posted Because The Service Has Been Destroyed")
                }
            }
        }

        override fun deliveryComplete(token: IMqttDeliveryToken?) {
            Logger.d("Connection[$clientId] Publishing Message<${token?.message
                    ?: ""}> Has Been Completed")
        }
    }

    private class InitializeSubscriptionListener(val weakSelf: WeakReference<PaperCupPhone>, val gate: CountDownLatch) : IMqttActionListener {
        override fun onSuccess(asyncActionToken: IMqttToken?) {
            val self = weakSelf.get() ?: return
            self.mCallback.isInitialized = true
            self.mSubscriptionTopic += self.mInitializeSubscriptionTopic
            self.mSubscriptionQoS += self.mInitializeSubscriptionQoS
            Logger.d("Topics${Arrays.toString(self.mInitializeSubscriptionTopic)} Subscription Success\nCurrent: ${Arrays.toString(self.mSubscriptionTopic)},${Arrays.toString(self.mSubscriptionQoS)}")
            gate.countDown()
        }

        override fun onFailure(asyncActionToken: IMqttToken?, exception: Throwable?) {
            val self = weakSelf.get() ?: return
            Logger.w("Topics${Arrays.toString(self.mInitializeSubscriptionTopic)} Subscription Failed")
            gate.countDown()
        }
    }

    private class SubscriptionListener(val weakSelf: WeakReference<PaperCupPhone>, val topic: Array<String>, val qos: IntArray, val gate: CountDownLatch) : IMqttActionListener {
        override fun onSuccess(asyncActionToken: IMqttToken?) {
            val self = weakSelf.get() ?: return
            self.mSubscriptionTopic += topic
            self.mSubscriptionQoS += qos
            val cachedSubscriptionTopicList = self.mCachedSubscriptionTopic.toMutableList()
            topic.forEach {
                val index = cachedSubscriptionTopicList.indexOf(it)
                if (index == -1) return@forEach
                cachedSubscriptionTopicList.removeAt(index)
            }
            self.mCachedSubscriptionTopic = cachedSubscriptionTopicList.toTypedArray()

            val cachedQoSList = self.mCachedSubscriptionQoS.toMutableList()
            qos.forEach {
                val index = cachedQoSList.indexOf(it)
                if (index == -1) return@forEach
                cachedQoSList.removeAt(index)
            }
            self.mCachedSubscriptionQoS = IntArray(cachedQoSList.size) { cachedQoSList[it] }

            Logger.d("Topics${Arrays.toString(topic)} Subscription Success\nCurrent: ${Arrays.toString(self.mSubscriptionTopic)},${Arrays.toString(self.mSubscriptionQoS)}")
            gate.countDown()
        }

        override fun onFailure(asyncActionToken: IMqttToken?, exception: Throwable?) {
            val self = weakSelf.get() ?: return
            Logger.w("Topics${Arrays.toString(topic)} Subscription Failed")
            gate.countDown()
        }
    }

    private class ConnectToBrokerRunnable(val weakSelf: WeakReference<PaperCupPhone>) : Runnable {
        override fun run() {
            val self = weakSelf.get() ?: return
            self.connectToBroker()
        }
    }

    private class SubscriptionRunnable(val weakSelf: WeakReference<PaperCupPhone>, val event: PaperCupPhone.Event.Topic.Subscribe, val retryInterval: Int) : Runnable {
        override fun run() {
            val self = weakSelf.get() ?: return
            if (self.isDestroyed) return

            val validSubscriptionIndexArray = ArrayList<Int>()
            event.topic.forEachIndexed { index, element ->
                if (!self.mSubscriptionTopic.contains(element)) validSubscriptionIndexArray.add(index)
            }
            if (validSubscriptionIndexArray.size == 0) {
                Logger.w("Subscribe Duplicate Topics: ${Arrays.toString(event.topic)}")
                return
            }

            val validSubscriptionTopicsArrayList = ArrayList<String>()
            val validQoSArrayList = ArrayList<Int>()
            for (index in validSubscriptionIndexArray) {
                validSubscriptionTopicsArrayList.add(event.topic[index])
                validQoSArrayList.add(event.qos[index])
            }
            val validSubscriptionTopics: Array<String> = Array(validSubscriptionTopicsArrayList.size) { validSubscriptionTopicsArrayList[it] }
            val validQoSs = IntArray(validQoSArrayList.size) { validQoSArrayList[it] }

            whileloop@ while (!self.isDestroyed) {
                val gate = CountDownLatch(1)
                var isSuccess = false
                if (self.isConnectionCompletedOnce) {
                    try {
                        self.mMQTTAndroidClient.subscribe(validSubscriptionTopics, validQoSs, null, object : IMqttActionListener {
                            override fun onSuccess(asyncActionToken: IMqttToken?) {
                                isSuccess = true
                                self.mSubscriptionTopic += validSubscriptionTopics
                                self.mSubscriptionQoS += validQoSs
                                Logger.d("Topics${Arrays.toString(event.topic)} Subscription Success\nCurrent: ${Arrays.toString(self.mSubscriptionTopic)},${Arrays.toString(self.mSubscriptionQoS)}")
                                gate.countDown()
                            }

                            override fun onFailure(asyncActionToken: IMqttToken?, exception: Throwable?) {
                                Logger.w("Topics${Arrays.toString(event.topic)} Subscription Failed")
                                gate.countDown()
                            }
                        })
                    } catch (ex: MqttException) {
                        Toast.makeText(self.applicationContext, "Subscribe Error", Toast.LENGTH_LONG).show()
                        Logger.e(ex, "throwable")
                        break@whileloop
                    } catch (ex: NullPointerException) {
                        Logger.v("Service Has Been Destroyed And The Above Operations Will Be Cancelled")
                        break@whileloop
                    }
                } else {
                    Logger.i("Unable To Subscribe To Topics When Connection Has Not Been Successful")
                    gate.countDown()
                }
                gate.await()
                when (isSuccess) {
                    true -> break@whileloop
                    else -> {
                        Logger.w("Retry After $retryInterval Second Interval")
                        Thread.sleep((retryInterval * 1000).toLong())
                    }
                }
            }
        }
    }

    private class UnsubscribeRunnable(val weakSelf: WeakReference<PaperCupPhone>, val event: PaperCupPhone.Event.Topic.Unsubscribe, val retryInterval: Int) : Runnable {
        override fun run() {
            val self = weakSelf.get() ?: return
            whileloop@ while (!self.isDestroyed) {
                val gate = CountDownLatch(1)
                var isSuccess = false
                if (self.isConnectionCompletedOnce) {
                    try {
                        self.mMQTTAndroidClient.unsubscribe(event.topic, null, object : IMqttActionListener {
                            override fun onSuccess(asyncActionToken: IMqttToken?) {
                                isSuccess = true
                                Logger.d("Topics${Arrays.toString(event.topic)} Unsubscribe Successfully")
                                gate.countDown()
                            }

                            override fun onFailure(asyncActionToken: IMqttToken?, exception: Throwable?) {
                                Logger.w("Topics${Arrays.toString(event.topic)} Unsubscribe Failed")
                                gate.countDown()
                            }
                        })
                    } catch (ex: MqttException) {
                        Toast.makeText(self.applicationContext, "Unsubscribe Error", Toast.LENGTH_LONG).show()
                        Logger.e(ex, "throwable")
                        break@whileloop
                    } catch (ex: NullPointerException) {
                        Logger.v("Service Has Been Destroyed And The Above Operations Will Be Cancelled")
                        break@whileloop
                    }
                } else {
                    Logger.i("Unable To Unsubscribe To Topics When Connection Has Not Been Successful")
                    gate.countDown()
                }
                gate.await()
                when (isSuccess) {
                    true -> {
                        val newTopic = ArrayList<String>()
                        val newQoS = ArrayList<Int>()
                        val indexs = ArrayList<Int>()
                        forloop@ for (topic in event.topic) {
                            val index = self.mSubscriptionTopic.indexOf(topic)
                            if (index == -1) continue@forloop
                            indexs.add(index)
                        }
                        if (indexs.size != event.topic.size) {
                            Logger.w("Unsubscribe Topics Does Not Match The Topics Of Current Subscribed ${Arrays.toString(self.mSubscriptionTopic)}, Retry After $retryInterval Second Interval")
                            Thread.sleep((retryInterval * 1000).toLong())
                            continue@whileloop
                        }
                        self.mSubscriptionTopic.forEachIndexed { index, element ->
                            if (indexs.contains(index)) return@forEachIndexed
                            newTopic.add(element)
                            newQoS.add(self.mSubscriptionQoS[index])
                        }
                        self.mSubscriptionTopic = Array(newTopic.size) { newTopic[it] }
                        self.mSubscriptionQoS = IntArray(newQoS.size) { newQoS[it] }
                        Logger.d("Current: ${Arrays.toString(self.mSubscriptionTopic)},${Arrays.toString(self.mSubscriptionQoS)}")
                        break@whileloop
                    }
                    false -> {
                        Logger.w("Retry After $retryInterval Second Interval")
                        Thread.sleep((retryInterval * 1000).toLong())
                    }
                }
            }
        }
    }

    private class PublishMessageRunnable(val weakSelf: WeakReference<PaperCupPhone>, val event: PaperCupPhone.Event.Topic.PublishMessage, val retryInterval: Int) : Runnable {
        override fun run() {
            val self = weakSelf.get() ?: return
            whileloop@ while (!self.isDestroyed) {
                var isSuccess = false
                if (self.isConnectionCompletedOnce) {
                    try {
                        val message = MqttMessage()
                        message.payload = event.message.toByteArray()
                        message.qos = event.qos
                        message.isRetained = event.isRetained
                        self.mMQTTAndroidClient.publish(event.topic, message) // If publish message is not instance of MqttMessage, it cannot store in message buffer
                        Logger.d("Publish Message <${event.message}> to <${event.topic}>")
                        if (!self.mMQTTAndroidClient.isConnected) {
                            Logger.v("There are ${self.mMQTTAndroidClient.bufferedMessageCount} messages in buffer.")
                        }
                        isSuccess = true
                    } catch (ex: MqttPersistenceException) {
                        Logger.e(ex, "When a problem occurs storing the message")
                        break@whileloop
                    } catch (ex: IllegalArgumentException) {
                        Logger.e(ex, "If value of QoS is not 0, 1 or 2")
                        break@whileloop
                    } catch (ex: MqttException) {
                        Logger.e(ex, "For other errors encountered while publishing the message. For instance, too many messages are being processed.")
                        break@whileloop
                    } catch (ex: NullPointerException) {
                        Logger.v("Service Has Been Destroyed And The Above Operations Will Be Cancelled")
                        break@whileloop
                    }
                } else {
                    Logger.i("Unable To Publish Message When Connection Has Not Been Successful")
                }
                when (isSuccess) {
                    true -> break@whileloop
                    else -> {
                        Logger.w("Retry After $retryInterval Second Interval")
                        Thread.sleep((retryInterval * 1000).toLong())
                    }
                }
            }
        }
    }

    data class Launcher(val brokerURI: String, val topicPrefix: String, val topic: Array<String>, val qos: IntArray, val isAutomaticReconnect: Boolean, val isCleanSession: Boolean, val keepAliveInterval: Int, val retryInterval: Int) : Serializable {
        companion object {
            const val name = "Launcher"
        }
    }

    sealed class Event {
        data class IncomingMessage(val jsonObject: JSONObject) : Event()
        object GetConnectionStatus : Event()

        sealed class Topic {
            data class Subscribe(val topic: Array<String>, val qos: IntArray) : Topic()
            data class Unsubscribe(val topic: Array<String>) : Topic()
            data class PublishMessage(val topic: String, val message: String, val qos: Int, val isRetained: Boolean) : Topic()
        }
    }
}