package com.kevincheng.papercupphone.paho

import android.content.Context
import com.orhanobut.logger.Logger
import org.eclipse.paho.android.service.MqttAndroidClient

class MqttAndroidClientExtended(context: Context, serverURI: String, clientId: String) : MqttAndroidClient(context, serverURI, clientId) {
    fun disconnectImmediately() {
        try {
            Logger.t("PAPER_CUP_PHONE").i("Connection[$clientId] Trying To Disconnect From The Broker")
            disconnect(0)
            Logger.t("PAPER_CUP_PHONE").i("Connection[$clientId] Disconnected From The Broker")
        } catch (ex: Exception) {
            if (ex is NullPointerException) return
            Logger.t("PAPER_CUP_PHONE").e(ex, "throwable")
        }
    }
}