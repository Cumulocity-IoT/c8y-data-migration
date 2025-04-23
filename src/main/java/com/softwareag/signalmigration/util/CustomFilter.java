package com.softwareag.signalmigration.util;

import java.util.Date;

import org.joda.time.DateTime;

import com.cumulocity.model.DateConverter;
import com.cumulocity.model.idtype.GId;
import com.cumulocity.model.util.ExtensibilityConverter;
import com.cumulocity.rest.representation.inventory.ManagedObjectRepresentation;
import com.cumulocity.sdk.client.Filter;
import com.cumulocity.sdk.client.ParamSource;
import com.cumulocity.sdk.client.measurement.MeasurementFilter;

public class CustomFilter extends MeasurementFilter  {

    @ParamSource
    private String fragmentType;

    @ParamSource
    private String valueFragmentType;

    @ParamSource
    private String valueFragmentSeries;

    @ParamSource
    private String dateFrom;

    @ParamSource
    private String dateTo;

    @ParamSource
    private String type;

    @ParamSource
    private String source;

    /**
     * Specifies the {@code type} query parameter
     *
     * @param type the type of the event(s)
     * @return the event filter with {@code type} set
     */
    public CustomFilter byType(String type) {
        this.type = type;
        return this;
    }

    /**
     * Specifies the {@code source} query parameter
     *
     * @param id the managed object that generated the event(s)
     * @return the event filter with {@code source} set
     */
    public CustomFilter bySource(GId id) {
        this.source = id.getValue();
        return this;
    }
    
    /**
     * Specifies the {@code source} query parameter
     *
     * @param source the managed object that generated the event(s)
     * @return the event filter with {@code source} set
     */
    @Deprecated
    public CustomFilter bySource(ManagedObjectRepresentation source) {
        this.source = source.getId().getValue();
        return this;
    }

    /**
     * @return the {@code type} parameter of the query
     */
    public String getType() {
        return type;
    }

    /**
     * @return the {@code source} parameter of the query
     */
    public String getSource() {
        return source;
    }

    public CustomFilter byFragmentType(Class<?> fragmentType) {
        this.fragmentType = ExtensibilityConverter.classToStringRepresentation(fragmentType);
        return this;
    }

    public CustomFilter byFragmentType(String fragmentType) {
        this.fragmentType = fragmentType;
        return this;
    }

    public String getFragmentType() {
        return fragmentType;
    }

    /**
     * Specify value fragment type. This is preferred over the parameter {@code fragmentType}, because working with
     * structured data and filtering via this parameter is lighter than filtering via {@code fragmentType}.
     *
     * @param valueFragmentType the value fragment type to filter.
     * @return the event filter with {@code valueFragmentType} set.
     * @see #byValueFragmentSeries(String)
     * @see #byValueFragmentTypeAndSeries(String, String)
     */
    public CustomFilter byValueFragmentType(String valueFragmentType) {
        this.valueFragmentType = valueFragmentType;
        return this;
    }

    /**
     * Specify value fragment series, usually use in conjunction with {@link #byValueFragmentType(String)}
     *
     * @param valueFragmentSeries value fragment series to filter.
     * @return the event filter with {@code valueFragmentSeries} set.
     * @see #byValueFragmentType(String)
     * @see #byValueFragmentTypeAndSeries(String, String)
     */
    public CustomFilter byValueFragmentSeries(String valueFragmentSeries) {
        this.valueFragmentSeries = valueFragmentSeries;
        return this;
    }

    /**
     * A short version combining of {@link #byValueFragmentType(String)} and {@link #byValueFragmentSeries(String)}.
     *
     * @param valueFragmentType value fragment type to filter, example: {@code c8y_TemperatureMeasurement}
     * @param valueFragmentSeries value fragment series to filter, example: {@code T}
     * @return the event filter with {@code valueFragmentType} and {@code valueFragmentSeries} set.
     */
    public CustomFilter byValueFragmentTypeAndSeries(String valueFragmentType, String valueFragmentSeries) {
        this.valueFragmentType = valueFragmentType;
        this.valueFragmentSeries = valueFragmentSeries;
        return this;
    }

    public String getValueFragmentType() {
        return valueFragmentType;
    }

    public String getValueFragmentSeries() {
        return valueFragmentSeries;
    }

    public CustomFilter byDate(Date fromDate, Date toDate) {
//        this.dateFrom = DateConverter.date2String(fromDate);    	
//        this.dateTo = DateConverter.date2String(toDate);
    	this.dateFrom = DateUtil.toISODateTimeString(fromDate);
    	this.dateTo = DateUtil.toISODateTimeString(toDate);
        return this;
    }

    public CustomFilter byFromDate(Date fromDate) {
        //this.dateFrom = DateConverter.date2String(fromDate);
    	this.dateFrom = DateUtil.toISODateTimeString(fromDate);
        return this;
    }

    public String getFromDate() {
        return dateFrom;
    }

    public String getToDate() {
        return dateTo;
    }



}
