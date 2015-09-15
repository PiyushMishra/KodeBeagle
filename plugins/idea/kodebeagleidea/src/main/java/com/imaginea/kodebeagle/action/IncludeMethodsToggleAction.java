package com.imaginea.kodebeagle.action;

import com.imaginea.kodebeagle.object.WindowObjects;
import com.intellij.icons.AllIcons;
import com.intellij.openapi.actionSystem.AnActionEvent;
import com.intellij.openapi.actionSystem.ToggleAction;

public class IncludeMethodsToggleAction extends ToggleAction {

    private static final String INCLUDE_METHODS_TEXT = "Include method calls";
    private static final String INCLUDE_METHODS_DESC = "Include method calls in your search";
    private final WindowObjects windowObjects = WindowObjects.getWindowObjects();

    public IncludeMethodsToggleAction() {
        super(INCLUDE_METHODS_TEXT, INCLUDE_METHODS_DESC, AllIcons.Nodes.Method);
    }

    @Override
    public final boolean isSelected(final AnActionEvent e) {
       return windowObjects.retrieveIncludeMethods();
    }

    @Override
    public final void setSelected(final AnActionEvent e, final boolean state) {
        windowObjects.saveIncludeMethods(state);
    }
}
