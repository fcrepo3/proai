package proai.util;

import proai.SetInfo;

import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

public abstract class SetSpec {

    public static boolean hasParents(String spec) {

        return spec != null && spec.contains(":");
    }

    public static String parentOf(String spec) {
        int boundary = spec.lastIndexOf(':');
        if (boundary > 0) {
            return spec.substring(0, boundary);
        } else {
            return null;
        }
    }

    public static boolean isValid(String spec) {
        return spec
                .matches("([A-Za-z0-9_!'$\\(\\)\\+\\-\\.\\*])+(:[A-Za-z0-9_!'$\\(\\)\\+\\-\\.\\*]+)*");
    }

    public static Set<String> allSetsFor(String spec) {
        Set<String> ancestors = new HashSet<String>();

        ancestors.add(spec);
        if (hasParents(spec)) {
            String parent = parentOf(spec);
            if (parent != null) {
                ancestors.add(parent);
                ancestors.addAll(SetSpec.allSetsFor(parent));
            }
        }

        return ancestors;
    }

    public static SetInfo defaultInfoFor(final String setSpec) {
        return new SetInfo() {

            public void write(PrintWriter w) {
                w.println("<set>");
                w.println("  <setSpec>" + setSpec + "</setSpec>");
                w.println("  <setName>" + setSpec + "</setName>");
                w.println("</set>");
            }

            public String getSetSpec() {
                return setSpec;
            }
        };
    }
}
