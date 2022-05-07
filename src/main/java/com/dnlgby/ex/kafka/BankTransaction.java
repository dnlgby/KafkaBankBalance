package com.dnlgby.ex.kafka;

import com.google.gson.annotations.SerializedName;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransaction {

    private static final String[] names = {"Daniel", "John", "Rany", "Avi", "Shalom", "Netanel"};
    private static final int maxAmount = 10_000;
    private static final String timePattern = "yyyy-MM-dd HH:mm:ss.SSS";

    @SerializedName("name")
    private String mName;
    @SerializedName("amount")
    private Double mAmount;
    @SerializedName("time")
    private String mTime;

    private BankTransaction(String name, double amount, String time){
        this.mName = name;
        this.mAmount = amount;
        this.mTime = time;
    }

    public String getName() {
        return mName;
    }

    public Double getAmount() {
        return mAmount;
    }

    public static BankTransaction createRandom(){
        String randomName = names[ThreadLocalRandom.current().nextInt(names.length)];
        int randomAmount = ThreadLocalRandom.current().nextInt(0, maxAmount + 1);
        String time = new SimpleDateFormat(timePattern).format(new Date());
        return new BankTransaction(randomName, randomAmount, time);
    }

}
