package org.opencdmp.deposit.ckan.validation;

import gr.cite.tools.exception.MyValidationException;
import tools.jackson.databind.JsonNode;

import java.util.*;
import java.util.function.Function;

/** Turns problems, the plugin's own or the "error" CKAN answers with, into one message named by the review's labels. */
public final class CkanErrors {

    public static final String INVALID_VALUES = "The repository does not accept some of the dataset's values.";

    private static final String ERROR_TYPE = "__type";
    private static final String ERROR_MESSAGE = "message";
    private static final String VALIDATION_ERROR = "Validation Error";
    private static final String EXTRAS = "extras";

    /** CKAN's core dataset fields by the review label the user edits them under. */
    private static final Map<String, String> LABELS = Map.ofEntries(
            Map.entry("name", "Identifier"),
            Map.entry("title", "Title"),
            Map.entry("notes", "Description"),
            Map.entry("version", "Version"),
            Map.entry("url", "Source URL"),
            Map.entry("tags", "Tags"),
            Map.entry("license_id", "License"),
            Map.entry("owner_org", "Organization"),
            Map.entry("private", "Private dataset"),
            Map.entry("author", "Author"),
            Map.entry("author_email", "Author email"),
            Map.entry("maintainer", "Maintainer"),
            Map.entry("maintainer_email", "Maintainer email"),
            Map.entry(EXTRAS, "Extra fields"),
            Map.entry("extras_validation", "Extra fields"),
            Map.entry("resources", "Files"));

    private CkanErrors() {
    }

    public static String label(String field) {
        return LABELS.getOrDefault(field, field);
    }

    public static MyValidationException exception(String intro, Map<String, List<String>> problems) {
        StringBuilder message = new StringBuilder(intro);
        List<Map.Entry<String, List<String>>> errors = new ArrayList<>();
        for (Map.Entry<String, List<String>> problem : problems.entrySet()) {
            for (String text : problem.getValue()) message.append(' ').append(problem.getKey()).append(": ").append(text.endsWith(".") ? text : text + ".");
            errors.add(Map.entry(problem.getKey(), problem.getValue()));
        }
        return new MyValidationException(message.toString(), errors);
    }

    public static boolean isValidationError(JsonNode error) {
        return error != null && VALIDATION_ERROR.equals(error.path(ERROR_TYPE).asString(null));
    }

    /** The text of a non-validation error, e.g. {"__type": "Authorization Error", "message": "Access denied"}. */
    public static String message(JsonNode error) {
        if (error == null) return null;
        String message = error.path(ERROR_MESSAGE).asString(null);
        if (message != null && !message.isBlank()) return message;
        return error.path(ERROR_TYPE).asString(null);
    }

    /**
     * The problems of a CKAN validation error: {"name": ["That URL is already in use."], "extras": [{"key": ["..."]}]}.
     * A custom field is named by its review label when it has one, an extra by its key.
     */
    public static Map<String, List<String>> fromCkan(JsonNode error, Function<String, String> customLabels, List<String> extraKeys) {
        Map<String, List<String>> problems = new LinkedHashMap<>();
        if (error == null) return problems;
        error.properties().forEach(entry -> {
            String field = entry.getKey();
            if (ERROR_TYPE.equals(field) || ERROR_MESSAGE.equals(field)) return;
            String label = LABELS.containsKey(field) ? LABELS.get(field) : Objects.requireNonNullElse(customLabels.apply(field), field);
            JsonNode value = entry.getValue();
            if (EXTRAS.equals(field) && value.isArray()) {
                for (int i = 0; i < value.size(); i++) {
                    String key = extraKeys != null && i < extraKeys.size() ? extraKeys.get(i) : null;
                    collect(value.get(i), key != null ? label + " '" + key + "'" : label, problems);
                }
            } else {
                collect(value, label, problems);
            }
        });
        return problems;
    }

    private static void collect(JsonNode node, String label, Map<String, List<String>> problems) {
        if (node == null || node.isNull()) return;
        if (node.isString()) {
            List<String> messages = problems.computeIfAbsent(label, k -> new ArrayList<>());
            if (!messages.contains(node.asString())) messages.add(node.asString());
        } else if (node.isArray()) {
            for (JsonNode item : node) collect(item, label, problems);
        } else if (node.isObject()) {
            // a nested item, e.g. a tag or an extra: its own fields' messages
            node.properties().forEach(entry -> collect(entry.getValue(), label, problems));
        }
    }
}
