/*
 * Copyright (C) 2023 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.flink.bigquery.table.restrictions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.NestedFieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.types.DataType;

import org.junit.Test;

import java.util.Arrays;
import java.util.Optional;

import static com.google.common.truth.Truth8.assertThat;

/** Tests for the {@link BigQueryRestriction} conversion of filters on nested fields. */
public class BigQueryNestedRestrictionTest {

    @Test
    public void testNestedLikePrefixPatternIsPushedDown() {
        DataType stringType = DataTypes.STRING().notNull();
        NestedFieldReferenceExpression field =
                createNestedField(new String[] {"address", "city"}, new int[] {1, 0}, stringType);
        ValueLiteralExpression literal = createLiteral("Tor%", stringType);

        Optional<String> result =
                BigQueryRestriction.convert(createLikeCallExpression(field, literal));

        assertThat(result).hasValue("address.city LIKE 'Tor%'");
    }

    @Test
    public void testNestedFieldEqualsLiteral() {
        NestedFieldReferenceExpression field =
                createNestedField(
                        new String[] {"address", "city"}, new int[] {1, 0}, DataTypes.STRING());
        ValueLiteralExpression literal =
                this.<String>createLiteral("Toronto", DataTypes.STRING().notNull());

        Optional<String> result =
                BigQueryRestriction.convert(createEqualsCallExpression(field, literal));

        assertThat(result).hasValue("(address.city = 'Toronto')");
    }

    @Test
    public void testNestedFieldEqualsLiteralWithLiteralOnLeft() {
        ValueLiteralExpression literal = createLiteral("Toronto", DataTypes.STRING().notNull());
        NestedFieldReferenceExpression field =
                createNestedField(
                        new String[] {"address", "city"}, new int[] {1, 0}, DataTypes.STRING());

        Optional<String> result =
                BigQueryRestriction.convert(createEqualsCallExpression(literal, field));

        assertThat(result).hasValue("('Toronto' = address.city)");
    }

    @Test
    public void testNestedFieldIsNull() {
        NestedFieldReferenceExpression field =
                createNestedField(
                        new String[] {"address", "city"}, new int[] {1, 0}, DataTypes.STRING());

        Optional<String> result =
                BigQueryRestriction.convert(
                        createCallExpression(
                                "isNull",
                                BuiltInFunctionDefinitions.IS_NULL,
                                DataTypes.BOOLEAN(),
                                field));

        assertThat(result).hasValue("address.city IS NULL");
    }

    @Test
    public void testNestedFieldIsNotNull() {
        NestedFieldReferenceExpression field =
                createNestedField(
                        new String[] {"address", "city"}, new int[] {1, 0}, DataTypes.STRING());

        Optional<String> result =
                BigQueryRestriction.convert(
                        createCallExpression(
                                "isNotNull",
                                BuiltInFunctionDefinitions.IS_NOT_NULL,
                                DataTypes.BOOLEAN(),
                                field));

        assertThat(result).hasValue("NOT address.city IS NULL");
    }

    @Test
    public void testNestedFieldGreaterThanLiteral() {
        NestedFieldReferenceExpression field =
                createNestedField(
                        new String[] {"address", "zip"}, new int[] {1, 1}, DataTypes.INT());
        ValueLiteralExpression literal = createLiteral(10000, DataTypes.INT().notNull());

        Optional<String> result =
                BigQueryRestriction.convert(createGreaterThanCallExpression(field, literal));

        assertThat(result).hasValue("(address.zip > 10000)");
    }

    @Test
    public void testNestedAnd() {
        NestedFieldReferenceExpression city =
                createNestedField(
                        new String[] {"address", "city"}, new int[] {1, 0}, DataTypes.STRING());
        NestedFieldReferenceExpression zip =
                createNestedField(
                        new String[] {"address", "zip"}, new int[] {1, 1}, DataTypes.INT());

        Optional<String> result =
                BigQueryRestriction.convert(
                        createAndCallExpression(
                                createEqualsCallExpression(
                                        city,
                                        createLiteral("Toronto", DataTypes.STRING().notNull())),
                                createGreaterThanCallExpression(
                                        zip, createLiteral(10000, DataTypes.INT().notNull()))));

        assertThat(result).hasValue("((address.city = 'Toronto') AND (address.zip > 10000))");
    }

    @Test
    public void testNestedOr() {
        NestedFieldReferenceExpression city =
                createNestedField(
                        new String[] {"address", "city"}, new int[] {1, 0}, DataTypes.STRING());
        NestedFieldReferenceExpression country =
                createNestedField(
                        new String[] {"address", "country"}, new int[] {1, 1}, DataTypes.STRING());

        Optional<String> result =
                BigQueryRestriction.convert(
                        createOrCallExpression(
                                createEqualsCallExpression(
                                        city,
                                        createLiteral("Toronto", DataTypes.STRING().notNull())),
                                createEqualsCallExpression(
                                        country,
                                        createLiteral("CA", DataTypes.STRING().notNull()))));

        assertThat(result).hasValue("((address.city = 'Toronto') OR (address.country = 'CA'))");
    }

    @Test
    public void testMultiLevelNestedFieldEqualsLiteral() {
        NestedFieldReferenceExpression field =
                createNestedField(
                        new String[] {"customer", "address", "city"},
                        new int[] {3, 1, 0},
                        DataTypes.STRING());
        ValueLiteralExpression literal = createLiteral("Toronto", DataTypes.STRING().notNull());

        Optional<String> result =
                BigQueryRestriction.convert(createEqualsCallExpression(field, literal));

        assertThat(result).hasValue("(customer.address.city = 'Toronto')");
    }

    @Test
    public void testNotNestedFieldEqualsLiteral() {
        NestedFieldReferenceExpression field =
                createNestedField(
                        new String[] {"address", "city"}, new int[] {1, 0}, DataTypes.STRING());
        ValueLiteralExpression literal = createLiteral("Toronto", DataTypes.STRING().notNull());
        CallExpression equalsCall = createEqualsCallExpression(field, literal);

        Optional<String> result =
                BigQueryRestriction.convert(
                        createCallExpression(
                                "not",
                                BuiltInFunctionDefinitions.NOT,
                                DataTypes.BOOLEAN(),
                                equalsCall));

        assertThat(result).hasValue("NOT (address.city = 'Toronto')");
    }

    private FieldReferenceExpression createField(String name, DataType type) {
        return new FieldReferenceExpression(name, type, 0, 0);
    }

    private NestedFieldReferenceExpression createNestedField(
            String[] fieldNames, int[] fieldIndices, DataType type) {
        return new NestedFieldReferenceExpression(fieldNames, fieldIndices, type);
    }

    private <T> ValueLiteralExpression createLiteral(T value, DataType type) {
        return new ValueLiteralExpression(value, type);
    }

    private CallExpression createEqualsCallExpression(
            ResolvedExpression left, ResolvedExpression right) {
        return createCallExpression(
                "equals", BuiltInFunctionDefinitions.EQUALS, DataTypes.BOOLEAN(), left, right);
    }

    private CallExpression createGreaterThanCallExpression(
            ResolvedExpression left, ResolvedExpression right) {
        return createCallExpression(
                "greaterThan",
                BuiltInFunctionDefinitions.GREATER_THAN,
                DataTypes.BOOLEAN(),
                left,
                right);
    }

    private CallExpression createLikeCallExpression(
            ResolvedExpression left, ResolvedExpression right) {
        return createCallExpression(
                "like", BuiltInFunctionDefinitions.LIKE, DataTypes.BOOLEAN(), left, right);
    }

    private CallExpression createAndCallExpression(
            ResolvedExpression left, ResolvedExpression right) {
        return createCallExpression(
                "and", BuiltInFunctionDefinitions.AND, DataTypes.BOOLEAN(), left, right);
    }

    private CallExpression createOrCallExpression(
            ResolvedExpression left, ResolvedExpression right) {
        return createCallExpression(
                "or", BuiltInFunctionDefinitions.OR, DataTypes.BOOLEAN(), left, right);
    }

    private CallExpression createCallExpression(
            String name,
            FunctionDefinition functionDefinition,
            DataType outputDataType,
            ResolvedExpression... children) {
        return new CallExpression(
                false,
                FunctionIdentifier.of(name),
                functionDefinition,
                Arrays.asList(children),
                outputDataType);
    }
}
