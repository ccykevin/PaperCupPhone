package com.kevincheng.papercupphoneexample

import android.support.v7.app.AppCompatActivity
import android.os.Bundle
import com.kevincheng.papercupphone.PaperCupPhoneAdapter
import com.kevincheng.papercupphone.PaperCupPhone
import kotlinx.android.synthetic.main.activity_main.*
import org.greenrobot.eventbus.EventBus
import org.greenrobot.eventbus.Subscribe
import org.greenrobot.eventbus.ThreadMode
import org.json.JSONException

class MainActivity : AppCompatActivity() {

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        EventBus.getDefault().register(this)

        button_subscribe.setOnClickListener { PaperCupPhoneAdapter.subscribeTopic("testing/", 1) }
        button_unsubscribe.setOnClickListener { PaperCupPhoneAdapter.unsubscribeTopic("testing/") }

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
    }

    @Subscribe(threadMode = ThreadMode.MAIN)
    fun onMessageEvent(message: PaperCupPhone.Event.IncomingMessage) {
        val jsonObject = message.jsonObject
        try {
            val data = jsonObject.getString("data")
            textview_helloworld.text = data
        } catch (ex: JSONException) { }
    }
}
