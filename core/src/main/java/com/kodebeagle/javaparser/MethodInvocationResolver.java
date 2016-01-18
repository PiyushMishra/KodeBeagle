package com.kodebeagle.javaparser;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.jdt.core.dom.ClassInstanceCreation;
import org.eclipse.jdt.core.dom.Expression;
import org.eclipse.jdt.core.dom.MethodDeclaration;
import org.eclipse.jdt.core.dom.MethodInvocation;
import org.eclipse.jdt.core.dom.SimpleName;
import org.eclipse.jdt.core.dom.SingleVariableDeclaration;
import org.eclipse.jdt.core.dom.Type;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;

public class MethodInvocationResolver extends TypeResolver {

	private Map<String, List<MethodInvokRef>> methodInvoks = new HashMap<String, List<MethodInvokRef>>();
	private Stack<MethodDeclaration> methodStack = new Stack<MethodDeclaration>();
	private List<MethodDecl> declaredMethods = new ArrayList<MethodDecl>();
	private List<TypeDecl> typeDeclarations = new ArrayList<>();
	private MethodInvokRef currentMethodInvokRef;
	private static final String OBJECT_TYPE = "java.lang.Object";
	protected Map<String,String> types = new HashMap();
	protected Queue<String> typesInFile = new ArrayDeque<>();

	public List<TypeDecl> getTypeDeclarations() {
		return typeDeclarations;
	}

	public Map<String, List<MethodInvokRef>> getMethodInvoks() {
		return methodInvoks;
	}

	public List<MethodDecl> getDeclaredMethods() {
		return declaredMethods;
	}

	protected MethodInvokRef getCurrentMethodInvokRef() {
		return currentMethodInvokRef;
	}

	String type = "";

	@Override
	public boolean visit(org.eclipse.jdt.core.dom.TypeDeclaration td) {
		if (typesInFile.isEmpty()) {
			type = "";
		}
		typesInFile.add(td.getName().getFullyQualifiedName());
		TypeDecl obj = new TypeDecl(td.getName().getFullyQualifiedName(), td.getStartPosition());
		typeDeclarations.add(obj);
		return true;
	}

	public Map<String, String> getTypes() {
		return types;
	}

	@Override
	public void endVisit(org.eclipse.jdt.core.dom.TypeDeclaration td) {
		if (!typesInFile.isEmpty()) {
			String xyz = typesInFile.remove();
			type = type + xyz + ".";
			types.put(xyz, currentPackage + "." + type.substring(0, type.lastIndexOf(".")));
		}
	}

	@Override
	public boolean visit(MethodDeclaration node) {
		methodStack.push(node);
		addMethodDecl(node);
		return super.visit(node);
	}

	@Override
	public void endVisit(MethodDeclaration node) {
		if (!methodStack.isEmpty()) {
			methodStack.pop();
		}
		super.endVisit(node);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean visit(ClassInstanceCreation node) {
		List args = node.arguments();
		Map<String, Integer> scopeBindings = getNodeScopes().get(node);
		List<String> argTypes = translateArgsToTypes(args, scopeBindings);
		String type = getNameOfType(node.getType());
		if (!methodStack.empty()) {
			MethodDeclaration currentMethod = methodStack.peek();
			String currMethodName = currentMethod.getName().toString();
			List<MethodInvokRef> invoks = methodInvoks.get(currMethodName);
			if (invoks == null) {
				invoks = new ArrayList<MethodInvokRef>();
				methodInvoks.put(currMethodName, invoks);
			}
			MethodInvokRef methodInvokRef = new MethodInvokRef("<init>", type, "", args
					.size(), node.getStartPosition(), argTypes);
			invoks.add(methodInvokRef);
			currentMethodInvokRef = methodInvokRef;
		}
		return super.visit(node);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean visit(MethodInvocation node) {
		SimpleName methodName = node.getName();
		List args = node.arguments();
		Expression expression = node.getExpression();

		Map<String, Integer> scopeBindings = getNodeScopes().get(node);
		String target = getTarget(expression);
		String targetType = translateTargetToType(expression, scopeBindings);
		List<String> argTypes = translateArgsToTypes(args, scopeBindings);
		if (!methodStack.empty()) {
			MethodDeclaration currentMethod = methodStack.peek();
			String currMethodName = currentMethod.getName().toString();
			List<MethodInvokRef> invoks = methodInvoks.get(currMethodName);
			if (invoks == null) {
				invoks = new ArrayList<MethodInvokRef>();
				methodInvoks.put(currMethodName, invoks);
			}
			MethodInvokRef methodInvokRef = new MethodInvokRef(methodName.toString(), targetType, target, args
					.size(), node.getName().getStartPosition(), argTypes);
			invoks.add(methodInvokRef);
			currentMethodInvokRef = methodInvokRef;
		}
		return true;
	}

	@SuppressWarnings("rawtypes")
	private void addMethodDecl(MethodDeclaration node) {
		SimpleName nameNode = node.getName();
		String methodName = nameNode.toString();
		List params = node.parameters();
		int num = params.size();
		List<String> paramTypes = new ArrayList<String>();
		for (Object p : params) {
			String typeName = OBJECT_TYPE;
			if (p instanceof SingleVariableDeclaration) {
				SingleVariableDeclaration svd = (SingleVariableDeclaration) p;
				Type type = svd.getType();
				typeName = getNameOfType(type);
			} else {
				System.err.println("Unxepected AST node type for param - " + p);
			}
			paramTypes.add(typeName);
		}
		declaredMethods.add(new MethodDecl(methodName, num, nameNode
				.getStartPosition(), paramTypes));
	}

	protected String translateTargetToType(Expression expression,
			Map<String, Integer> scopeBindings) {
		String targetType = "";
		if (expression != null) {
			String target = getTarget(expression);
			final Integer variableId = scopeBindings.get(target);
			if (variableId == null
					|| !getVariableBinding().containsKey(variableId)) {

				String staticTypeRef = getImportedNames().get(target);
				if (staticTypeRef != null) {
					targetType = "<static>" + staticTypeRef;
				} else {
					//System.out.println("Ignoring target " + target);
				}
			} else {
				targetType = getVariableTypes().get(variableId);
			}
		}
		return targetType;
	}

	private String getTarget(Expression expression) {
		String target = "";
		if(expression != null){
			target = expression.toString();
			if (target.contains("this.")) {
				target = StringUtils.substringAfter(target, "this.");
			}
		}
		return target;
	}

	@SuppressWarnings("rawtypes")
	protected List<String> translateArgsToTypes(List args,
			Map<String, Integer> scopeBindings) {
		List<String> argTypes = new ArrayList<String>();
		for (Object o : args) {
			String name = o.toString();
			Integer varId = scopeBindings.get(name);
			if (varId == null) {
				String staticTypeRef = getImportedNames().get(name);
				if (staticTypeRef != null) {
					argTypes.add("<static>" + staticTypeRef);
				} else {
					argTypes.add(OBJECT_TYPE);
				}
			} else {
				argTypes.add(getVariableTypes().get(varId));
			}
		}
		return argTypes;
	}

	public static class TypeDecl {
		private String className;
		private Integer loc;

		public TypeDecl(String className, Integer loc) {
			super();
			this.className = className;
			this.loc = loc;
		}

		public String getClassName() {
			return className;
		}

		public Integer getLoc() {
			return loc;
		}
	}

		public static class MethodDecl {
		private String methodName;
		private Integer argNum;
		private Integer location;
		private List<String> argTypes;

		public MethodDecl(String methodName, Integer argNum, Integer location,
				List<String> argTypes) {
			super();
			this.methodName = methodName;
			this.argNum = argNum;
			this.location = location;
			this.argTypes = argTypes;
		}

		public String getMethodName() {
			return methodName;
		}

		public Integer getArgNum() {
			return argNum;
		}

		public Integer getLocation() {
			return location;
		}

		public List<String> getArgTypes() {
			return argTypes;
		}

		@Override
		public String toString() {
			return "MethodDecl [methodName=" + methodName + ", argNum="
					+ argNum + ", location=" + location + ", argTypes="
					+ argTypes + "]";
		}

	}

	public static class MethodInvokRef {
		private String methodName;
		private String targetType;
		private String target;
		private Integer argNum;
		private Integer location;
		private List<String> argTypes;
		

		private MethodInvokRef(String methodName, String targetType, String target,
				Integer argNum, Integer location, List<String> argTypes) {
			super();
			this.methodName = methodName;
			this.targetType = targetType;
			this.argNum = argNum;
			this.location = location;
			this.argTypes = argTypes;
			this.target = target;
		}

		public String getMethodName() {
			return methodName;
		}

		public void setMethodName(String methodName) {
			this.methodName = methodName;
		}

		public String getTargetType() {
			return targetType;
		}

		public void setTargetType(String targetType) {
			this.targetType = targetType;
		}

		
		public String getTarget() {
			return target;
		}

		public void setTarget(String target) {
			this.target = target;
		}

		public Integer getArgNum() {
			return argNum;
		}

		public void setArgNum(Integer argNum) {
			this.argNum = argNum;
		}

		public Integer getLocation() {
			return location;
		}

		public void setLocation(Integer location) {
			this.location = location;
		}

		public List<String> getArgTypes() {
			return argTypes;
		}

		public void setArgTypes(List<String> argTypes) {
			this.argTypes = argTypes;
		}

		@Override
		public String toString() {
			return "MethodInvokRef [methodName=" + methodName + ", targetType="
					+ targetType + ", target=" + target + ", argNum=" + argNum
					+ ", location=" + location + ", argTypes=" + argTypes + "]";
		}
	}
}
