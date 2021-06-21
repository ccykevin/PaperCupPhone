package com.kevincheng.papercupphoneexample

import android.app.Application
import android.content.Context
import android.os.Build
import android.os.HandlerThread
import androidx.multidex.MultiDex
import com.orhanobut.logger.AndroidLogAdapter
import com.orhanobut.logger.Logger

class PaperCupPhoneExampleApplication : Application() {
    override fun attachBaseContext(base: Context?) {
        super.attachBaseContext(base)

        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.LOLLIPOP) MultiDex.install(this)
    }

    override fun onCreate() {
        super.onCreate()
        val mLoggerHandlerThread = HandlerThread("PaperCupPhoneExampleApplication-Logger")
        mLoggerHandlerThread.start()
        Logger.addLogAdapter(AndroidLogAdapter())
    }
}