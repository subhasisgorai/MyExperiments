package com.webworkz.playground.azure.common;

public abstract class Constants {
    private Constants() {
    	throw new AssertionError();
    }

    public static final String PARTITION_KEY = "PartitionKey";
    public static final String ROW_KEY = "RowKey";
    public static final String NOT_NULL_VALIDATION_ERROR_MESSAGE = "[%s] Object passed was null";
    public static final String WHITESPACE = " ";
    public static final String PIPE = "|";
    public static final String PARENTHESES_START = "(";
    public static final String PARENTHESES_END = ")";
    
}
