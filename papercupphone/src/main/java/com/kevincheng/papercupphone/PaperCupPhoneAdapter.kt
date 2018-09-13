package com.kevincheng.papercupphone

import android.content.Context
import android.content.Intent
import com.orhanobut.logger.Logger
import org.greenrobot.eventbus.EventBus

class PaperCupPhoneAdapter {
    companion object {
        fun connect(context: Context, brokerURI: String, topicPrefix: String, subscriptionTopics: Array<String>, qosArray: IntArray, isAutomaticReconnect: Boolean, isCleanSession: Boolean, keepAliveInterval: Int) {
            // If already running, it will restart service
            disconnect(context)
            val intent = Intent(context, PaperCupPhone::class.java)
            val launcher = PaperCupPhone.Launcher(brokerURI, topicPrefix, subscriptionTopics, qosArray, isAutomaticReconnect, isCleanSession, keepAliveInterval)
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
                    Logger.e("Is Not Running Yet")
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
                    Logger.e("Is Not Running Yet")
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
                    Logger.e("Is Not Running Yet")
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
                    Logger.e("Is Not Running Yet")
                    false
                }
            }
        }
    }
}