/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kodebeagle.parser;

import com.kodebeagle.javaparser.JavaASTParser;
import com.kodebeagle.javaparser.JavaASTParser.ParseType;
import com.kodebeagle.javaparser.MethodInvocationResolver;
import com.kodebeagle.javaparser.MethodInvocationResolver.MethodDecl;
import com.kodebeagle.javaparser.MethodInvocationResolver.MethodInvokRef;
import com.kodebeagle.javaparser.SingleClassBindingResolver;
import org.eclipse.jdt.core.dom.ASTNode;
import org.eclipse.jdt.core.dom.CompilationUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ASTParserTest extends AbstractParseTest {
    private CompilationUnit unit;
    private SingleClassBindingResolver resolver;

    @Before
    public void setup() {
        setTestConfig();
    }

    //TODO: call setTestConfig() once in the testsuite
    private void setTestConfig() {
        JavaASTParser pars = new JavaASTParser(true);
        ASTNode cu = pars.getAST(oneMethod, ParseType.COMPILATION_UNIT);
        unit = (CompilationUnit) cu;
        resolver = new SingleClassBindingResolver(unit);
        resolver.resolve();
    }

    @Test
    public void testClassesInFile() {
        Collection<String> expectedClassesInFile = new ArrayList<String>();

        expectedClassesInFile.add("x.y.z.ABC");
        expectedClassesInFile.add("x.y.z.DefaultRequestDirector.DEF");
        expectedClassesInFile.add("x.y.z.DefaultRequestDirector.DEF.GHI");
        expectedClassesInFile.add("x.y.z.DefaultRequestDirector");

        Collection<String> actualClassesInFile = resolver.getClassesInFile().values();
        Assert.assertEquals(expectedClassesInFile.toArray(), actualClassesInFile.toArray());
    }

    @Test
    public void testImportNamesInFile() {
        //TODO: Some imports are missed
        /*setTestConfig();
        System.out.println(resolver.getResolver().getImportedNames());*/
    }

    @Test
    public void testFullyQualifiedName() {
        String expected = "org.apache.commons.logging.Log";
        String actual = resolver.getResolver().getFullyQualifiedNameFor("Log");
        Assert.assertEquals(expected, actual);
        expected = "java.lang.String";
        actual = resolver.getResolver().getFullyQualifiedNameFor("String");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testGetVariableTypes() {
        oneMethod = "import x.y.Type;" +
                "import org.apache.Duration;" +
                "class Abc { Type xyz = new Type();" +
                "Duration obj = new Duration();" +
                "obj.print(); }";
        setTestConfig();
        Map<String, String> expected = new HashMap<>();
        expected.put("obj", "org.apache.Duration");
        expected.put("xyz", "x.y.Type");
        Map<String, String> actual = resolver.getVariableTypes();
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testGetDeclaredMethods() {
        oneMethod = "import x.y.Type;" +
                "import org.apache.Duration;" +
                "class Abc { Type xyz = new Type();" +
                "Duration obj = new Duration();" +
                "void display(){" +
                "Duration inMethod = new Duration();" +
                "inMethod.display(); } }";
        setTestConfig();
        MethodDecl expected = new MethodDecl("display", 0, 112, new ArrayList<String>());
        MethodDecl actual = resolver.getDeclaredMethods().get(0);
        Assert.assertTrue(expected.getMethodName().equals(actual.getMethodName()) && expected.getArgNum() == actual.getArgNum()
                && expected.getLocation() == actual.getLocation() && expected.getArgTypes().containsAll(actual.getArgTypes()));

    }

    @Test
    public void testGetTypesAtPosition() {
        oneMethod = readInputStream(this.getClass().getResourceAsStream(
                "/TestData.java"));
        setTestConfig();
        Map<Integer, String> actual = new HashMap<>();
        Map<Integer, String> expected = new HashMap<>();
        expected.put(62, "x.y.Type");
        expected.put(77, "x.y.Type");
        expected.put(89, "org.apache.Duration");
        expected.put(108, "org.apache.Duration");
        Map<ASTNode, String> actualTemp = resolver.getTypesAtPosition();
        for (Map.Entry<ASTNode, String> entry : actualTemp.entrySet()) {
            actual.put(entry.getKey().getStartPosition(), entry.getValue());
        }
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testGetVariableTypesAtPosition() {
        oneMethod = readInputStream(this.getClass().getResourceAsStream(
                "/TestData.java"));
        setTestConfig();
        Map<Integer, String> actual = new HashMap<>();
        Map<Integer, String> expected = new HashMap<>();
        expected.put(67, "x.y.Type");
        expected.put(98, "org.apache.Duration");
        expected.put(150, "org.apache.Duration");
        Map<ASTNode, String> actualTemp = resolver.getVariableTypesAtPosition();
        for (Map.Entry<ASTNode, String> entry : actualTemp.entrySet()) {
            actual.put(entry.getKey().getStartPosition(), entry.getValue());
        }
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testOneMethod() {
        System.out.println("filetypes in given file");
        Map<String, String> types = resolver.getClassesInFile();
        for (MethodInvocationResolver.TypeDecl typeDeclaration : resolver.getTypeDeclarations()) {
            System.out.println(types.get(typeDeclaration.getClassName()) + "   "
                    + unit.getLineNumber(typeDeclaration.getLoc()) + "  " + unit.getColumnNumber(typeDeclaration.getLoc()));
        }

        Map<ASTNode, String> typesAtPos = resolver.getVariableTypesAtPosition();

        for (Entry<ASTNode, String> e : typesAtPos.entrySet()) {
            Integer line = unit.getLineNumber(e.getKey().getStartPosition());
            Integer col = unit.getColumnNumber(e.getKey().getStartPosition());
            System.out.println(line + " , " + col + " , " + e.getKey().getLength() + " : " + e.getValue());
        }

        for (Entry<ASTNode, String> e : resolver.getTypesAtPosition()
                .entrySet()) {
            Integer line = unit.getLineNumber(e.getKey().getStartPosition());
            Integer col = unit.getColumnNumber(e.getKey().getStartPosition());
            System.out.println(line + " , " + col + " , " + e.getKey().getLength() + " : " + e.getValue());
        }

        for (Entry<ASTNode, ASTNode> e : resolver.getVariableDependencies()
                .entrySet()) {
            ASTNode child = e.getKey();
            Integer chline = unit.getLineNumber(child.getStartPosition());
            Integer chcol = unit.getColumnNumber(child.getStartPosition());
            Integer chLength = child.getLength();
            ASTNode parent = e.getValue();
            Integer pline = unit.getLineNumber(parent.getStartPosition());
            Integer pcol = unit.getColumnNumber(parent.getStartPosition());
            Integer plength = parent.getLength();
            System.out.println("**** " + child + "[" + chline + ", " + chcol + ", " + chLength
                    + "] ==> " + parent.toString() + "[" + pline + ", " + pcol + ", " + plength
                    + "]");
        }

        for (Entry<String, List<MethodInvokRef>> entry : resolver
                .getMethodInvoks().entrySet()) {
            System.out.println(" ~~~~~~~~~~~ For method " + entry.getKey()
                    + " ~~~~~~~~~~~");
            for (MethodInvokRef m : entry.getValue()) {
                Integer loc = m.getLocation();
                Integer line = unit.getLineNumber(loc);
                Integer col = unit.getColumnNumber(loc);
                System.out.println("[" + line + ", " + col  + ", "  + m.getLength()+" ] ==> " + m);
            }
        }

        for (MethodDecl m : resolver.getDeclaredMethods()) {
            System.out
                    .println("~~~~~~~~~~~~~~~~~ Declared Methods ~~~~~~~~~~~~~~~~~");
            System.out.println(m);
        }
        System.out.println(resolver.getVariableTypes());
    }
}
