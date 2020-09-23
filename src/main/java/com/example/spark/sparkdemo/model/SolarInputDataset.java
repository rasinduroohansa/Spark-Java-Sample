package com.example.spark.sparkdemo.model;

public class SolarInputDataset {
    Integer rowId;
    String month;
    Integer day;
    Integer hour;
    Double beamIrradiance;
    Double diffuseIrradiance;
    Double ambientTemperature;
    Double windSpeed;
    Double planeofArrayIrradiance;
    Double cellTemperature;
    Double dCArrayOutput;
    Double aCSystemOutput;

    public Integer getRowId() {
        return rowId;
    }

    public void setRowId(Integer rowId) {
        this.rowId = rowId;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public Integer getDay() {
        return day;
    }

    public void setDay(Integer day) {
        this.day = day;
    }

    public Integer getHour() {
        return hour;
    }

    public void setHour(Integer hour) {
        this.hour = hour;
    }

    public Double getBeamIrradiance() {
        return beamIrradiance;
    }

    public void setBeamIrradiance(Double beamIrradiance) {
        this.beamIrradiance = beamIrradiance;
    }

    public Double getDiffuseIrradiance() {
        return diffuseIrradiance;
    }

    public void setDiffuseIrradiance(Double diffuseIrradiance) {
        this.diffuseIrradiance = diffuseIrradiance;
    }

    public Double getAmbientTemperature() {
        return ambientTemperature;
    }

    public void setAmbientTemperature(Double ambientTemperature) {
        this.ambientTemperature = ambientTemperature;
    }

    public Double getWindSpeed() {
        return windSpeed;
    }

    public void setWindSpeed(Double windSpeed) {
        this.windSpeed = windSpeed;
    }

    public Double getPlaneofArrayIrradiance() {
        return planeofArrayIrradiance;
    }

    public void setPlaneofArrayIrradiance(Double planeofArrayIrradiance) {
        this.planeofArrayIrradiance = planeofArrayIrradiance;
    }

    public Double getCellTemperature() {
        return cellTemperature;
    }

    public void setCellTemperature(Double cellTemperature) {
        this.cellTemperature = cellTemperature;
    }

    public Double getdCArrayOutput() {
        return dCArrayOutput;
    }

    public void setdCArrayOutput(Double dCArrayOutput) {
        this.dCArrayOutput = dCArrayOutput;
    }

    public Double getaCSystemOutput() {
        return aCSystemOutput;
    }

    public void setaCSystemOutput(Double aCSystemOutput) {
        this.aCSystemOutput = aCSystemOutput;
    }
}
