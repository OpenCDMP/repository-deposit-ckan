package org.opencdmp.deposit.ckan.validation;

import org.opencdmp.deposit.ckan.model.CkanDataset;
import org.opencdmp.deposit.ckan.model.CkanExtra;
import org.opencdmp.deposit.ckan.model.CkanTag;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Checks a dataset against CKAN's core dataset schema (CKAN 2.10 default_create_package_schema) before it is sent: the
 * dataset name, version length, tags, author and maintainer e-mails, and extras. Every installation applies these;
 * the rules of an installation's own fields are left to it, and its answer is reported as it gives it.
 */
@Component
public class CkanDatasetValidator {

    // ckan.logic.validators: name_match, tag_name_validator and email_pattern
    private static final Pattern NAME = Pattern.compile("[a-z0-9_\\-]*");
    private static final Pattern TAG = Pattern.compile("[\\w \\-.]*", Pattern.UNICODE_CHARACTER_CLASS);
    private static final Pattern EMAIL = Pattern.compile("^(?!\\.)(?!.*\\.$)(?!.*?\\.\\.)[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$");
    private static final Set<String> RESERVED_NAMES = Set.of("new", "edit", "search");
    private static final int NAME_MIN_LENGTH = 2;
    private static final int NAME_MAX_LENGTH = 100;
    private static final int VERSION_MAX_LENGTH = 100;
    private static final int TAG_MIN_LENGTH = 2;
    private static final int TAG_MAX_LENGTH = 100;
    /** The core schema's own fields, which an extra cannot be named after (extra_key_not_in_root_schema). */
    private static final Set<String> ROOT_FIELDS = Set.of("id", "name", "title", "author", "author_email", "maintainer", "maintainer_email", "license_id", "notes", "url",
            "version", "state", "type", "owner_org", "private", "resources", "tags", "tag_string", "plugin_data", "extras", "groups", "relationships_as_object", "relationships_as_subject");

    /** The problems by review label (e.g. "Tags"), in review order; empty when CKAN's core schema accepts it. */
    public Map<String, List<String>> validate(CkanDataset dataset) {
        Map<String, List<String>> problems = new LinkedHashMap<>();
        String name = dataset.getName();
        if (name == null || name.isBlank()) add(problems, "name", "An identifier is needed.");
        else if (RESERVED_NAMES.contains(name)) add(problems, "name", "'" + name + "' cannot be used as an identifier.");
        else if (name.length() < NAME_MIN_LENGTH || name.length() > NAME_MAX_LENGTH) add(problems, "name", "The identifier must be " + NAME_MIN_LENGTH + " to " + NAME_MAX_LENGTH + " characters long.");
        else if (!NAME.matcher(name).matches()) add(problems, "name", "'" + name + "' can only contain lowercase letters (a-z), digits, hyphens (-) and underscores (_).");

        if (dataset.getVersion() != null && dataset.getVersion().length() > VERSION_MAX_LENGTH) add(problems, "version", "At most " + VERSION_MAX_LENGTH + " characters.");

        if (dataset.getTags() != null) {
            for (CkanTag tag : dataset.getTags()) {
                String value = tag != null ? tag.getName() : null;
                if (value == null || value.isEmpty()) continue;
                if (value.length() < TAG_MIN_LENGTH || value.length() > TAG_MAX_LENGTH) add(problems, "tags", "'" + value + "' must be " + TAG_MIN_LENGTH + " to " + TAG_MAX_LENGTH + " characters long.");
                else if (!TAG.matcher(value).matches()) add(problems, "tags", "'" + value + "' can only contain letters, digits, spaces, hyphens (-), underscores (_) and dots (.).");
            }
        }

        this.validateEmail("author_email", dataset.getAuthorEmail(), problems);
        this.validateEmail("maintainer_email", dataset.getMaintainerEmail(), problems);

        if (dataset.getExtras() != null) {
            Set<String> keys = new HashSet<>();
            for (CkanExtra extra : dataset.getExtras()) {
                if (extra == null) continue;
                String key = extra.getKey();
                if (key == null || key.isBlank()) add(problems, "extras", "An extra field needs a key.");
                else if (ROOT_FIELDS.contains(key)) add(problems, "extras", "'" + key + "' is a field of the dataset itself and cannot be an extra field key.");
                else if (!keys.add(key)) add(problems, "extras", "The key '" + key + "' is used more than once.");
            }
        }
        return problems;
    }

    private void validateEmail(String field, String value, Map<String, List<String>> problems) {
        if (value == null || value.isBlank()) return;
        if (!EMAIL.matcher(value.strip()).matches()) add(problems, field, "'" + value + "' is not a valid e-mail address.");
    }

    private static void add(Map<String, List<String>> problems, String field, String message) {
        problems.computeIfAbsent(CkanErrors.label(field), k -> new ArrayList<>()).add(message);
    }
}
