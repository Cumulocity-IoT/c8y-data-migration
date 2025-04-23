package com.softwareag.signalmigration.util;

import com.cumulocity.sdk.client.Filter;
import com.cumulocity.sdk.client.Param;
import com.cumulocity.sdk.client.QueryParam;

import lombok.Getter;

/**
 * Sample usage:
 * 
 * String query = "$filter=_id gt '100'";
 * final QueryParam q = CustomQueryParam.QUERY.setValue(query).toQueryParam();
 *  
 */
public enum CustomQueryParam implements Param {
	WITH_TOTAL_PAGES("withTotalPages"),
	CURRENT_PAGE("currentPage"),
	WITH_CHILDREN("withChildren"),
	PAGE_SIZE("pageSize"),
	QUERY("query"),
	DEVICE_QUERY("q"),
	DATE_FROM("dateFrom"),
	DATE_TO("dateTo"),
	STATUS("status"),
	FRAGMENT_TYPE("fragmentType"),
	DEVICE_ID("deviceId"),
	REVERT("revert"),
	SOURCE("source"), 
	RESOLVED("resolved"), // alarms query
	VALUE_FRAGMENT_TYPE("valueFragmentType")
	;

	private String name;
	
	@Getter
	private String value;

	private CustomQueryParam(final String name) {
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}

	public CustomQueryParam setValue(final String value) {
		this.value = value;
		return this;
	}

	public QueryParam toQueryParam() {
		return new QueryParam(this, Filter.encode(value));
	}

}
