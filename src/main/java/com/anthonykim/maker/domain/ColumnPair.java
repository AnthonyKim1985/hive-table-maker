package com.anthonykim.maker.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-19.
 */
@Data
@AllArgsConstructor
public class ColumnPair {
    private String columnName;
    private String columnType;
}