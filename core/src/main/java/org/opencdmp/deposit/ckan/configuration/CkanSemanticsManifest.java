package org.opencdmp.deposit.ckan.configuration;

import org.opencdmp.commonmodels.enums.PluginEntityType;
import org.opencdmp.commonmodels.enums.SemanticKeyLevel;
import org.opencdmp.commonmodels.enums.SemanticValueFormat;
import org.opencdmp.commonmodels.models.semantics.SemanticKeyUsage;
import org.opencdmp.commonmodels.models.semantics.SemanticVocabulary;
import org.opencdmp.commonmodels.models.semantics.SemanticsManifest;
import org.opencdmp.deposit.ckan.service.ckan.LocalCustomFieldProperties;
import org.opencdmp.depositbase.enums.DataType;
import org.opencdmp.depositbase.repository.CustomFieldProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The semantics manifest the CKAN deposit publishes with its configuration: the dataset values it reads from
 * semantic keys, the relationships in {@code semantics.relationships-as-object} / {@code relationships-as-subject}
 * and one value per configured custom field.
 * <p>
 * Every value is "can use": the title, name, notes, author and maintainer always come from the plan and the owner
 * organization falls back to the configured one, so nothing a template or blueprint leaves out breaks a deposit. Keys
 * are read from blueprint and template inputs alike; a reference input gives its reference's code.
 */
public final class CkanSemanticsManifest {

    public static final String PREFIX = "ckan";

    private static final String CUSTOM_FIELD_PREFIX = "ckan.custom_fields.";

    // kept in step with CkanBuilder: these custom fields are written once per repeated answer, as <jPath>_1, _2, ...
    private static final Set<String> INDEXED_CUSTOM_FIELD_CODES = Set.of("creator_first_name", "creator_last_name", "creator_orcid_id");

    private static final List<PluginEntityType> ALL = List.of(PluginEntityType.Plan, PluginEntityType.Description);

    private CkanSemanticsManifest() {
    }

    public static SemanticsManifest build(SemanticsProperties semantics, LocalCustomFieldProperties customFields) {
        List<SemanticKeyUsage> usages = new ArrayList<>();

        usages.add(usage(List.of("ckan.dataset.source"), ALL, "Dataset › Source",
                "Written to the dataset's source URL.", "url", SemanticValueFormat.Uri, false,
                "The plan's public page, for a public plan"));
        usages.add(usage(List.of("ckan.dataset.organization"), ALL, "Dataset › Organization",
                "Sets the owner organization; it must be the name (not the id) of an organization of the CKAN instance. Any other value is ignored and the configured organization is used, without trying the plan's references.",
                "owner_org", SemanticValueFormat.Text, false,
                "The first organisation reference of the plan whose code is a CKAN organization name, then the configured organization"));
        usages.add(usage(List.of("ckan.dataset.license"), ALL, "Dataset › License",
                "Sets the licence; it must be a CKAN licence id (not a URL). Any other value is ignored and the organisation's licence is used, without trying the plan's references.",
                "license_id", SemanticValueFormat.Text, false,
                "The first licence reference of the plan that CKAN knows, then the organisation's licence"));
        usages.add(usage(List.of("ckan.dataset.tags"), ALL, "Dataset › Tags",
                "Each answer becomes a tag when it holds only letters, digits, spaces and . _ -; description tags are always added.",
                "tags[].name", SemanticValueFormat.Text, true, null));
        usages.add(usage(List.of("ckan.dataset.groups"), ALL, "Dataset › Groups",
                "Each answer joins the dataset to the CKAN group of that name or id, besides the organisation's groups.",
                "groups[].name", SemanticValueFormat.Text, true, null));
        usages.add(usage(List.of("ckan.dataset.extras"), ALL, "Dataset › Extras",
                "Each answered input becomes an extra keyed by the input's id; an input answered in several assets gives one extra with its distinct answers joined by commas.",
                "extras[]", SemanticValueFormat.Text, true, null));

        List<String> asObject = distinct(semantics == null ? null : semantics.getRelationshipsAsObject());
        if (!asObject.isEmpty()) {
            usages.add(usage(asObject, ALL, "Dataset › Relationships (as object)",
                    "Each answer creates a relationship from this dataset to the given one, typed by the key's last part; the value must be an existing CKAN dataset id or name.",
                    "package_relationship_create (subject = this dataset)", SemanticValueFormat.Text, true, null));
        }
        List<String> asSubject = distinct(semantics == null ? null : semantics.getRelationshipsAsSubject());
        if (!asSubject.isEmpty()) {
            usages.add(usage(asSubject, ALL, "Dataset › Relationships (as subject)",
                    "Each answer creates a relationship from the given dataset to this one, typed by the key's last part; the value must be an existing CKAN dataset id or name.",
                    "package_relationship_create (object = this dataset)", SemanticValueFormat.Text, true, null));
        }

        List<CustomFieldProperties.CustomField> fields = customFields == null || customFields.getCustomFields() == null ? List.of() : customFields.getCustomFields();
        for (CustomFieldProperties.CustomField field : fields) {
            if (field == null || field.getCode() == null || field.getCode().isBlank()) continue;
            String code = field.getCode();
            String target = field.getjPath() != null && !field.getjPath().isBlank() ? field.getjPath() : code;
            SemanticValueFormat format = format(field.getDataType());
            String label = "Custom field › " + code;
            if (INDEXED_CUSTOM_FIELD_CODES.contains(code)) {
                usages.add(usage(List.of(CUSTOM_FIELD_PREFIX + code), List.of(PluginEntityType.Description), label,
                        "Written once per repeated answer of the description, as " + target + "_1, " + target + "_2, ...",
                        target + "_n", format, true, null));
            } else {
                usages.add(usage(List.of(CUSTOM_FIELD_PREFIX + code), ALL, label,
                        field.getIsArray() ? "Written as a list to the custom field " + target + "." : "Written to the custom field " + target + "; the first answer is used.",
                        target, format, field.getIsArray(), null));
            }
        }

        return new SemanticsManifest(List.of(new SemanticVocabulary(PREFIX, "CKAN", false)), usages);
    }

    private static SemanticKeyUsage usage(List<String> keys, List<PluginEntityType> entityTypes, String label, String purpose, String target, SemanticValueFormat format, boolean multiple, String fallback) {
        SemanticKeyUsage usage = new SemanticKeyUsage(keys, false, SemanticKeyLevel.CanUse, entityTypes, label, purpose, target);
        usage.setFormat(format);
        usage.setMultiple(multiple);
        usage.setFallback(fallback);
        return usage;
    }

    private static SemanticValueFormat format(DataType dataType) {
        if (dataType == null) return SemanticValueFormat.Text;
        return switch (dataType) {
            case Boolean -> SemanticValueFormat.Boolean;
            case Integer, Long -> SemanticValueFormat.Integer;
            case Double -> SemanticValueFormat.Number;
            default -> SemanticValueFormat.Text;
        };
    }

    private static List<String> distinct(List<String> keys) {
        return keys == null ? List.of() : keys.stream().filter(Objects::nonNull).filter(x -> !x.isBlank()).distinct().toList();
    }
}
