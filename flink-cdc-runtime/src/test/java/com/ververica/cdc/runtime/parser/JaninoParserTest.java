/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.runtime.parser;

import org.apache.flink.table.runtime.generated.CompileUtils;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.Java;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;
import org.codehaus.janino.Unparser;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

/** Unit tests for the {@link JaninoParser}. */
public class JaninoParserTest {

    @Test
    public void testJaninoParser() throws CompileException, IOException, InvocationTargetException {
        String expression = "1==2";
        Parser parser = new Parser(new Scanner(null, new StringReader(expression)));
        ExpressionEvaluator expressionEvaluator = new ExpressionEvaluator();
        expressionEvaluator.cook(parser);
        Object evaluate = expressionEvaluator.evaluate();
        Assert.assertEquals(evaluate, false);
    }

    @Test
    public void testJaninoUnParser()
            throws CompileException, IOException, InvocationTargetException {
        String expression = "1 <= 2";
        String[] values = new String[1];
        values[0] = "1";
        String value2 = "2";
        Java.AmbiguousName ambiguousName1 = new Java.AmbiguousName(Location.NOWHERE, values);
        Java.AmbiguousName ambiguousName2 =
                new Java.AmbiguousName(Location.NOWHERE, new String[] {value2});
        Java.BinaryOperation binaryOperation =
                new Java.BinaryOperation(Location.NOWHERE, ambiguousName1, "<=", ambiguousName2);
        StringWriter writer = new StringWriter();
        Unparser unparser = new Unparser(writer);
        unparser.unparseAtom(binaryOperation);
        unparser.close();
        Assert.assertEquals(writer.toString(), expression);
    }

    @Test
    public void testJaninoNumericCompare() throws InvocationTargetException {
        String expression = "col1==3.14";
        List<String> columnNames = Arrays.asList("col1");
        List<Class<?>> paramTypes = Arrays.asList(Double.class);
        List<Object> params = Arrays.asList(3.14);
        ExpressionEvaluator expressionEvaluator =
                CompileUtils.compileExpression(expression, columnNames, paramTypes, Boolean.class);
        Object evaluate = expressionEvaluator.evaluate(params.toArray());
        Assert.assertEquals(evaluate, true);
    }

    @Test
    public void testJaninoStringCompare() throws InvocationTargetException {
        String expression = "String.valueOf('2').equals(col1)";
        List<String> columnNames = Arrays.asList("col1");
        List<Class<?>> paramTypes = Arrays.asList(String.class);
        List<Object> params = Arrays.asList("2");
        ExpressionEvaluator expressionEvaluator =
                CompileUtils.compileExpression(expression, columnNames, paramTypes, Boolean.class);
        Object evaluate = expressionEvaluator.evaluate(params.toArray());
        Assert.assertEquals(evaluate, true);
    }

    @Test
    public void testTranslateFilterToJaninoExpression() {
        String expression = "abs(uniq_id) > 10 and id is not null";
        String janinoExpressionExpect = "abs(uniq_id) > 10 && null != id";
        String janinoExpression = JaninoParser.translateFilterToJaninoExpression(expression);
        Assert.assertEquals(janinoExpressionExpect, janinoExpression);
    }
}
