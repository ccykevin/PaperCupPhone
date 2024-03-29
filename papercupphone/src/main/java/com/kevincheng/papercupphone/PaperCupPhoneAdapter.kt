package com.kevincheng.papercupphone

import android.app.Service
import android.content.Context
import android.content.Intent
import com.orhanobut.logger.Logger
import org.eclipse.paho.client.mqttv3.MqttClient
import org.greenrobot.eventbus.EventBus

class PaperCupPhoneAdapter {
    companion object {
        fun generateClientId(): String {
            return MqttClient.generateClientId()
        }

        fun connect(context: Context, brokerURI: String, clientId: String,
                    isAutomaticReconnect: Boolean, isCleanSession: Boolean,
                    keepAliveInterval: Int, retryInterval: Int,
                    account: PaperCupPhone.Launcher.Account? = null,
                    will: PaperCupPhone.Launcher.Will? = null,
                    initialTopics: Array<String>? = null, initialQoSs: IntArray? = null,
                    startCommand: Int = Service.START_REDELIVER_INTENT,
                    foregroundConfig: PaperCupPhone.Launcher.ForegroundConfig? = null, isDebug: Boolean = false) {
            // If already running, it will restart service
            disconnect(context)
            val intent = Intent(context, PaperCupPhone::class.java)
            val topics: PaperCupPhone.Launcher.Topics? = when {
                initialTopics != null && initialQoSs != null && initialTopics.size == initialQoSs.size -> PaperCupPhone.Launcher.Topics(initialTopics, initialQoSs)
                else -> null
            }
            val launcher = PaperCupPhone.Launcher(PaperCupPhone.Launcher.Client(brokerURI, clientId), PaperCupPhone.Launcher.ConnectOptions(isAutomaticReconnect, isCleanSession, keepAliveInterval, retryInterval, account, will), topics, startCommand, foregroundConfig, isDebug)
            intent.putExtra(PaperCupPhone.Launcher.name, launcher)
            context.startService(intent)
        }

        fun disconnect(context: Context) {
            context.stopService(Intent(context, PaperCupPhone::class.java))
        }

        fun getConnectionStatus() : Boolean {
            return when {
                PaperCupPhone.isRunning -> {
                    EventBus.getDefault().post(PaperCupPhone.Event.GetConnectionStatus)
                    true
                }
                else -> {
                    Logger.t("PAPER_CUP_PHONE").e("Is Not Running Yet")
                    false
                }
            }
        }

        fun subscribeTopic(topic: String, qos: Int) : Boolean {
            return subscribeTopics(arrayOf(topic), IntArray(1) { qos })
        }

        fun subscribeTopics(topic: Array<String>, qos: IntArray) : Boolean {
            return when {
                PaperCupPhone.isRunning -> {
                    if (topic.size != qos.size) throw IllegalArgumentException()
                    EventBus.getDefault().post(PaperCupPhone.Event.Topic.Subscribe(topic, qos))
                    true
                }
                else -> {
                    Logger.t("PAPER_CUP_PHONE").e("Is Not Running Yet")
                    false
                }
            }
        }

        fun unsubscribeTopic(topic: String) : Boolean {
            return unsubscribeTopics(arrayOf(topic))
        }

        fun unsubscribeTopics(topic: Array<String>) : Boolean {
            return when {
                PaperCupPhone.isRunning -> {
                    EventBus.getDefault().post(PaperCupPhone.Event.Topic.Unsubscribe(topic))
                    true
                }
                else -> {
                    Logger.t("PAPER_CUP_PHONE").e("Is Not Running Yet")
                    false
                }
            }
        }

        fun publishMessage(topic: String, message: String, qos: Int, retained: Boolean) : Boolean {
            return when {
                PaperCupPhone.isRunning -> {
                    EventBus.getDefault().post(PaperCupPhone.Event.Topic.PublishMessage(topic, message, qos, retained))
                    true
                }
                else -> {
                    Logger.t("PAPER_CUP_PHONE").e("Is Not Running Yet")
                    false
                }
            }
        }
    }
}