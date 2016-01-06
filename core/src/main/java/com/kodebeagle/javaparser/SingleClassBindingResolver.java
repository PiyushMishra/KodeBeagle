/**
 *
 */
package com.kodebeagle.javaparser;

import com.google.common.collect.Maps;
import com.kodebeagle.javaparser.MethodInvocationResolver.MethodDecl;
import com.kodebeagle.javaparser.MethodInvocationResolver.MethodInvokRef;
import org.eclipse.jdt.core.dom.ASTNode;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Type resolution for a stand alone java source. 
 * 
 * @author sachint
 *
 */
public class SingleClassBindingResolver {

	private final ASTNode rootNode;

	private final MethodInvocationResolver resolver = new MethodInvocationResolver();

	public SingleClassBindingResolver(final ASTNode node) {
		rootNode = node;
	}

	/**
	 * Return a naive variableName to variableType map.
	 *
	 * @return
	 */
	public Map<String, String> getVariableTypes() {
		final Map<String, String> variableNameTypes = Maps.newTreeMap();
		for (final Entry<Integer, List<ASTNode>> variableBinding : resolver.getVariableBinding()
				.entrySet()) {
			final String varType = checkNotNull(resolver.getVariableTypes()
					.get(variableBinding.getKey()));
			for (final ASTNode node : variableBinding.getValue()) {
				variableNameTypes.put(node.toString(), varType);
			}
		}
		return variableNameTypes;
	}

	/**
	 * Returns the location and type of all the variables.
	 * @return
	 */
	public Map<Integer, String> getVariableTypesAtPosition() {
		final Map<Integer, String> variableTypes = Maps.newTreeMap();

		for (final Entry<Integer, List<ASTNode>> variableBinding : resolver.getVariableBinding()
				.entrySet()) {
			Integer bindingId = variableBinding.getKey();
			final String varType = checkNotNull(resolver.getVariableTypes()
					.get(bindingId));
			for (final ASTNode node : variableBinding.getValue()) {
				variableTypes.put(node.getStartPosition(), varType);
			}
		}
		return variableTypes;
	}
	
	public Map<ASTNode, ASTNode> getVariableDependencies(){

		final Map<ASTNode, ASTNode> variableTypes = Maps.newIdentityHashMap();

		for (final Entry<Integer, List<ASTNode>> variableBinding : resolver.getVariableBinding()
				.entrySet()) {
			Integer bindingId = variableBinding.getKey();
			final ASTNode parent = resolver.getVariableRefBinding().get(bindingId);
			for (final ASTNode node : variableBinding.getValue()) {
				variableTypes.put(node, parent);
			}
		}
		return variableTypes;
	}
	
	/**
	 * Returns the locations where a type is mentioned and its actual 
	 * fully qualified type name.
	 * @return
	 */
	public Map<Integer, String> getTypesAtPosition(){
		final Map<Integer, String> nodeTypes = Maps.newTreeMap();
		
		for (final Entry<ASTNode, String> typeBinding : resolver.getNodeTypeBinding()
				.entrySet()) {
			nodeTypes.put(typeBinding.getKey().getStartPosition(), typeBinding.getValue());
		}
		return nodeTypes;
	}
	
	public Map<String, List<MethodInvokRef>> getMethodInvoks() {
		return resolver.getMethodInvoks();
	}
	
	public List<MethodDecl> getDeclaredMethods() {
		return resolver.getDeclaredMethods();
	}

	public List<String> getClassesInFile() { return resolver.getTypes();}

	public void resolve() {
		rootNode.accept(resolver);
	}

}
