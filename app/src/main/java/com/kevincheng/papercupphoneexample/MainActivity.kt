package com.kevincheng.papercupphoneexample

import android.support.v7.app.AppCompatActivity
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import com.kevincheng.papercupphone.PaperCupPhoneAdapter
import com.kevincheng.papercupphone.PaperCupPhone
import com.orhanobut.logger.Logger
import kotlinx.android.synthetic.main.activity_main.*
import org.greenrobot.eventbus.EventBus
import org.greenrobot.eventbus.Subscribe
import org.greenrobot.eventbus.ThreadMode
import org.json.JSONException
import org.json.JSONObject

class MainActivity : AppCompatActivity() {

    private val mainHandler: Handler = Handler(Looper.getMainLooper())

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        EventBus.getDefault().register(this)

        button_subscribe.setOnClickListener { PaperCupPhoneAdapter.subscribeTopic("testing/", 1) }
        button_unsubscribe.setOnClickListener { PaperCupPhoneAdapter.unsubscribeTopic("testing/") }
        button_publish.setOnClickListener {
            mainHandler.removeCallbacksAndMessages(null)
            mainHandler.post(RepeatPublishMessageRunnable(mainHandler))
        }

        val brokerURI = "tcp://10.0.0.91:1889"
        val subscriptionTopics = arrayOf("wo/gq/all", "debug/")
        val qosArray = IntArray(subscriptionTopics.size) { 1 }
        val isAutoReconnect = true
        val isCleanSession = true
        val keepAliveInternal =  30
        val retryInterval = 15

        PaperCupPhoneAdapter.connect(this, brokerURI, "", subscriptionTopics, qosArray, isAutoReconnect, isCleanSession, keepAliveInternal, retryInterval)
    }

    override fun onDestroy() {
        super.onDestroy()
        EventBus.getDefault().unregister(this)
        mainHandler.removeCallbacksAndMessages(null)
    }

    @Subscribe(threadMode = ThreadMode.MAIN)
    fun onMessageEvent(event: PaperCupPhone.Event.IncomingMessage) {
        try {
            val jsonObject = JSONObject(event.message)
            Logger.json(jsonObject.toString())
            val data = jsonObject.getString("data")
            textview_helloworld.text = data
        } catch (ex: JSONException) { }
    }

    @Subscribe(threadMode = ThreadMode.MAIN)
    fun onMessageEvent(event: PaperCupPhone.Event.ConnectionStatus) {
        Logger.d("isConnected: ${event.isConnected}")
    }

    class RepeatPublishMessageRunnable(private val handler: Handler): Runnable {
        private var count = 0

        override fun run() {
            PaperCupPhoneAdapter.publishMessage("testing/", "{\"data\": \"testMessage${++count}\"}", 1, true)
            handler.postDelayed(this, 1000)
        }
    }
}
