package com.learningspark.part2structuredstreaming;
import java.util.Date;
import java.util.Objects;

public class Stock {
    private String company;
    private Date date;
    private double value;

    // Default constructor
    public Stock() {
    }

    // Parameterized constructor
    public Stock(String company, Date date, double value) {
        this.company = company;
        this.date = date;
        this.value = value;
    }

    // Getter and Setter for company
    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    // Getter and Setter for date
    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    // Getter and Setter for value
    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    // Override toString method
    @Override
    public String toString() {
        return "Stock{" +
                "company='" + company + '\'' +
                ", date=" + date +
                ", value=" + value +
                '}';
    }


}
