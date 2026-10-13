package org.opencdmp.deposit.ckan.model.builder;

import org.opencdmp.commonmodels.enums.PreviewFieldSource;
import org.opencdmp.commonmodels.enums.PreviewFieldType;
import org.opencdmp.commonmodels.models.deposit.DepositPreviewModel;
import org.opencdmp.commonmodels.models.deposit.PreviewField;
import org.opencdmp.commonmodels.models.deposit.PreviewFieldGroup;
import org.opencdmp.commonmodels.models.deposit.PreviewFieldOption;
import org.opencdmp.commonmodels.models.deposit.PreviewFieldValue;
import org.opencdmp.commonmodels.models.deposit.PreviewGroupItemModel;
import org.opencdmp.commonmodels.models.deposit.PreviewGroupValuesModel;
import org.opencdmp.deposit.ckan.configuration.preview.PreviewExtraFieldsProperties;
import org.opencdmp.deposit.ckan.model.CkanDataset;
import org.opencdmp.deposit.ckan.model.CkanExtra;
import org.opencdmp.deposit.ckan.model.CkanTag;
import org.opencdmp.depositbase.repository.DepositPluginSettings;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Maps a built {@link CkanDataset} to a deposit preview descriptor and applies
 * user-edited preview values back onto a built dataset before submission. Both
 * directions live in this class so the preview form and the deposited metadata
 * always derive from the same mapping.
 */
@Component
public class CkanPreviewMapper {

    private final PreviewExtraFieldsProperties previewExtraFieldsProperties;

    public CkanPreviewMapper(PreviewExtraFieldsProperties previewExtraFieldsProperties) {
        this.previewExtraFieldsProperties = previewExtraFieldsProperties;
    }

    public static final String FIELD_TITLE = "title";
    public static final String FIELD_NOTES = "notes";
    public static final String FIELD_VERSION = "version";
    public static final String FIELD_URL = "url";
    public static final String FIELD_TYPE = "type";
    public static final String FIELD_NAME = "name";
    public static final String FIELD_PRIVATE = "private";
    public static final String FIELD_LICENSE = "license";
    public static final String FIELD_OWNER_ORG = "owner-org";
    public static final String FIELD_AUTHOR = "author";
    public static final String FIELD_AUTHOR_EMAIL = "author-email";
    public static final String FIELD_MAINTAINER = "maintainer";
    public static final String FIELD_MAINTAINER_EMAIL = "maintainer-email";
    public static final String FIELD_TAGS = "tags";
    public static final String FIELD_FILES = "files";

    public static final String GROUP_EXTRAS = "extras";
    public static final String GROUP_CREATORS = "creators";
    public static final String GROUP_FILES_CODE = "files";
    public static final String GROUP_DOMAIN_SPECIFIC_CODE = "domain-specific";

    public static final String ITEM_EXTRA_KEY = "extra-key";
    public static final String ITEM_EXTRA_VALUE = "extra-value";
    public static final String ITEM_GIVEN_NAME = "given-name";
    public static final String ITEM_FAMILY_NAME = "family-name";
    public static final String ITEM_ORCID = "orcid";

    // custom field keys that CkanBuilder deposits as one CKAN key per repeatable-creator-row ordinal
    // (e.g. "creator_first_name_1", "creator_first_name_2", ...) instead of a single scalar value;
    // these are rendered as the dedicated repeatable "creators" group instead of plain domain-specific fields.
    private static final String CUSTOM_FIELD_CREATOR_FIRST_NAME = "creator_first_name";
    private static final String CUSTOM_FIELD_CREATOR_LAST_NAME = "creator_last_name";
    private static final String CUSTOM_FIELD_CREATOR_ORCID = "creator_orcid_id";
    private static final List<String> INDEXED_CREATOR_CUSTOM_FIELD_CODES = List.of(CUSTOM_FIELD_CREATOR_FIRST_NAME, CUSTOM_FIELD_CREATOR_LAST_NAME, CUSTOM_FIELD_CREATOR_ORCID);

    // the preview fields an organisation can lock, by the code of the setting that fills them
    private static final Map<String, String> LOCKABLE_FIELDS = Map.of(
            CkanBuilder.SETTING_ORGANISATION, FIELD_OWNER_ORG,
            CkanBuilder.SETTING_VISIBILITY, FIELD_PRIVATE,
            CkanBuilder.SETTING_LICENCE, FIELD_LICENSE,
            CkanBuilder.SETTING_MAINTAINER, FIELD_MAINTAINER,
            CkanBuilder.SETTING_MAINTAINER_EMAIL, FIELD_MAINTAINER_EMAIL);

    public DepositPreviewModel toPreview(CkanDataset dataset, List<String> uploadFileNames, DepositPluginSettings settings) {
        DepositPreviewModel model = new DepositPreviewModel();
        List<PreviewFieldGroup> groups = new ArrayList<>();
        if (uploadFileNames != null && !uploadFileNames.isEmpty()) groups.add(this.buildFilesGroup(uploadFileNames));
        groups.add(this.buildBasicInformationGroup(dataset));
        groups.add(this.buildAccessGroup(dataset));
        groups.add(this.buildPeopleGroup(dataset));
        if (this.hasIndexedCreatorFieldsConfigured()) groups.add(this.buildCreatorsGroup(dataset));
        groups.add(this.buildExtrasGroup(dataset));
        if (this.previewExtraFieldsProperties.getFields() != null && !this.previewExtraFieldsProperties.getFields().isEmpty()) groups.add(this.buildDomainSpecificGroup(dataset));
        groups.removeIf(group -> settings.isHidden(group.getCode()));
        for (PreviewFieldGroup group : groups) {
            for (PreviewField field : group.getFields()) {
                if (this.isLocked(field.getCode(), settings)) field.setSource(PreviewFieldSource.System);
            }
        }
        model.setGroups(groups);
        return model;
    }

    private PreviewFieldGroup buildBasicInformationGroup(CkanDataset dataset) {
        PreviewFieldGroup group = this.buildGroup("basic-information", "Basic information");
        List<PreviewField> fields = group.getFields();

        fields.add(this.buildField(FIELD_TITLE, "Title", PreviewFieldType.Text, PreviewFieldSource.Mapped, true,
                () -> this.textValue(FIELD_TITLE, dataset.getTitle())));
        PreviewField notes = this.buildField(FIELD_NOTES, "Description", PreviewFieldType.TextArea, PreviewFieldSource.Mapped, false,
                () -> this.textValue(FIELD_NOTES, dataset.getNotes()));
        fields.add(notes);
        fields.add(this.buildField(FIELD_VERSION, "Version", PreviewFieldType.Text, PreviewFieldSource.Mapped, false,
                () -> this.textValue(FIELD_VERSION, dataset.getVersion())));
        fields.add(this.buildField(FIELD_URL, "Source URL", PreviewFieldType.Text, PreviewFieldSource.Mapped, false,
                () -> this.textValue(FIELD_URL, dataset.getUrl())));

        PreviewField tags = this.buildField(FIELD_TAGS, "Tags", PreviewFieldType.Tags, PreviewFieldSource.Mapped, false,
                () -> this.textListValue(FIELD_TAGS, this.tagNames(dataset)));
        fields.add(tags);

        fields.add(this.buildField(FIELD_NAME, "Identifier", PreviewFieldType.ReadOnly, PreviewFieldSource.System, false,
                () -> this.textValue(FIELD_NAME, dataset.getName())));

        return group;
    }

    private PreviewFieldGroup buildAccessGroup(CkanDataset dataset) {
        PreviewFieldGroup group = this.buildGroup("access", "License and access");
        List<PreviewField> fields = group.getFields();

        PreviewField license = this.buildField(FIELD_LICENSE, "License", PreviewFieldType.Select, PreviewFieldSource.Mapped, false,
                () -> this.textValue(FIELD_LICENSE, dataset.getLicenseId()));
        license.setVocabSearch(true);
        fields.add(license);

        PreviewField ownerOrg = this.buildField(FIELD_OWNER_ORG, "Organization", PreviewFieldType.Select, PreviewFieldSource.Mapped, false,
                () -> this.textValue(FIELD_OWNER_ORG, dataset.getOwnerOrg()));
        ownerOrg.setVocabSearch(true);
        fields.add(ownerOrg);

        fields.add(this.buildField(FIELD_PRIVATE, "Private dataset", PreviewFieldType.Boolean, PreviewFieldSource.Mapped, false,
                () -> this.booleanValue(FIELD_PRIVATE, dataset.isPrivate())));

        return group;
    }

    private PreviewFieldGroup buildPeopleGroup(CkanDataset dataset) {
        PreviewFieldGroup group = this.buildGroup("people", "Author and maintainer");
        List<PreviewField> fields = group.getFields();

        fields.add(this.buildField(FIELD_AUTHOR, "Author", PreviewFieldType.Text, PreviewFieldSource.Mapped, false,
                () -> this.textValue(FIELD_AUTHOR, dataset.getAuthor())));
        fields.add(this.buildField(FIELD_AUTHOR_EMAIL, "Author email", PreviewFieldType.Text, PreviewFieldSource.Mapped, false,
                () -> this.textValue(FIELD_AUTHOR_EMAIL, dataset.getAuthorEmail())));
        fields.add(this.buildField(FIELD_MAINTAINER, "Maintainer", PreviewFieldType.Text, PreviewFieldSource.Mapped, false,
                () -> this.textValue(FIELD_MAINTAINER, dataset.getMaintainer())));
        fields.add(this.buildField(FIELD_MAINTAINER_EMAIL, "Maintainer email", PreviewFieldType.Text, PreviewFieldSource.Mapped, false,
                () -> this.textValue(FIELD_MAINTAINER_EMAIL, dataset.getMaintainerEmail())));

        return group;
    }

    /** True when the deployment has declared at least one of the indexed creator custom fields in preview-extra-fields. */
    private boolean hasIndexedCreatorFieldsConfigured() {
        if (this.previewExtraFieldsProperties.getFields() == null) return false;
        return this.previewExtraFieldsProperties.getFields().stream()
                .anyMatch(field -> field != null && field.getKey() != null && INDEXED_CREATOR_CUSTOM_FIELD_CODES.contains(field.getKey()));
    }

    /**
     * "creators" mirrors CkanBuilder's INDEXED_CUSTOM_FIELD_CODES handling: the custom fields
     * "creator_first_name", "creator_last_name" and "creator_orcid_id" are not plain scalars but are
     * deposited/read as one CKAN key per repeatable-creator ordinal (e.g. "creator_first_name_1",
     * "creator_first_name_2", ...). This group renders/collects them as rows instead of exposing three
     * disconnected domain-specific fields, so the pairing between a given creator's name and ORCID
     * survives editing.
     */
    private PreviewFieldGroup buildCreatorsGroup(CkanDataset dataset) {
        PreviewFieldGroup group = this.buildGroup(GROUP_CREATORS, "Creators");
        group.setRepeatable(true);
        List<PreviewField> fields = group.getFields();

        fields.add(this.buildTemplateField(ITEM_GIVEN_NAME, "First name", PreviewFieldType.Text));
        fields.add(this.buildTemplateField(ITEM_FAMILY_NAME, "Last name", PreviewFieldType.Text));
        fields.add(this.buildTemplateField(ITEM_ORCID, "ORCID", PreviewFieldType.Text));

        Map<Integer, String> firstNames = this.indexedCustomFieldValues(dataset, CUSTOM_FIELD_CREATOR_FIRST_NAME);
        Map<Integer, String> lastNames = this.indexedCustomFieldValues(dataset, CUSTOM_FIELD_CREATOR_LAST_NAME);
        Map<Integer, String> orcids = this.indexedCustomFieldValues(dataset, CUSTOM_FIELD_CREATOR_ORCID);

        Set<Integer> ordinals = new TreeSet<>();
        ordinals.addAll(firstNames.keySet());
        ordinals.addAll(lastNames.keySet());
        ordinals.addAll(orcids.keySet());

        List<PreviewGroupItemModel> items = new ArrayList<>();
        for (Integer ordinal : ordinals) {
            List<PreviewFieldValue> values = new ArrayList<>();
            values.add(this.textValue(ITEM_GIVEN_NAME, firstNames.get(ordinal)));
            values.add(this.textValue(ITEM_FAMILY_NAME, lastNames.get(ordinal)));
            values.add(this.textValue(ITEM_ORCID, orcids.get(ordinal)));
            PreviewGroupItemModel item = new PreviewGroupItemModel();
            item.setValues(values);
            items.add(item);
        }
        group.setItems(items);
        group.setBaseItems(items);
        return group;
    }

    /** Reads all "{key}_{ordinal}" custom field entries for the given base key, keyed by their 1-based ordinal. */
    private Map<Integer, String> indexedCustomFieldValues(CkanDataset dataset, String key) {
        Map<Integer, String> values = new TreeMap<>();
        if (dataset.getCustomFields() == null) return values;
        String prefix = key + "_";
        for (Map.Entry<String, Object> entry : dataset.getCustomFields().entrySet()) {
            if (entry.getKey() == null || !entry.getKey().startsWith(prefix) || entry.getValue() == null) continue;
            try {
                int ordinal = Integer.parseInt(entry.getKey().substring(prefix.length()));
                values.put(ordinal, entry.getValue().toString());
            } catch (NumberFormatException ignored) {
                // not an ordinal-suffixed key, e.g. some unrelated field that happens to share the prefix
            }
        }
        return values;
    }

    private PreviewFieldGroup buildExtrasGroup(CkanDataset dataset) {
        PreviewFieldGroup group = this.buildGroup(GROUP_EXTRAS, "Extra fields");
        group.setRepeatable(true);
        List<PreviewField> fields = group.getFields();

        fields.add(this.buildTemplateField(ITEM_EXTRA_KEY, "Key", PreviewFieldType.Text));
        fields.add(this.buildTemplateField(ITEM_EXTRA_VALUE, "Value", PreviewFieldType.Text));

        List<PreviewGroupItemModel> items = new ArrayList<>();
        if (dataset.getExtras() != null) {
            for (CkanExtra extra : dataset.getExtras()) {
                if (extra == null || extra.getKey() == null) continue;
                List<PreviewFieldValue> values = new ArrayList<>();
                values.add(this.textValue(ITEM_EXTRA_KEY, extra.getKey()));
                values.add(this.textValue(ITEM_EXTRA_VALUE, extra.getValue()));
                PreviewGroupItemModel item = new PreviewGroupItemModel();
                item.setValues(values);
                items.add(item);
            }
        }
        group.setItems(items);
        group.setBaseItems(items);
        return group;
    }

    private PreviewFieldGroup buildFilesGroup(List<String> uploadFileNames) {
        PreviewFieldGroup group = this.buildGroup(GROUP_FILES_CODE, "Files");
        group.getFields().add(this.buildField(FIELD_FILES, "Files to be uploaded", PreviewFieldType.ReadOnly, PreviewFieldSource.System, false,
                () -> this.textListValue(FIELD_FILES, uploadFileNames)));
        return group;
    }

    private PreviewFieldGroup buildDomainSpecificGroup(CkanDataset dataset) {
        PreviewFieldGroup group = this.buildGroup(GROUP_DOMAIN_SPECIFIC_CODE, "Domain specific fields");
        List<PreviewField> fields = group.getFields();
        for (PreviewExtraFieldsProperties.ExtraField extraField : this.previewExtraFieldsProperties.getFields()) {
            if (extraField == null || extraField.getKey() == null || extraField.getKey().isBlank()) continue;
            String key = extraField.getKey();
            if (INDEXED_CREATOR_CUSTOM_FIELD_CODES.contains(key)) continue; // rendered by the dedicated "creators" group instead
            String label = extraField.getLabel() != null && !extraField.getLabel().isBlank() ? extraField.getLabel() : key;
            PreviewFieldType type = this.extraFieldType(extraField.getType());
            fields.add(this.buildField(key, label, type, PreviewFieldSource.Manual, false,
                    () -> this.extraFieldValue(key, type, dataset)));
        }
        return group;
    }

    private PreviewFieldType extraFieldType(String type) {
        if (type == null) return PreviewFieldType.Text;
        return switch (type.toLowerCase()) {
            case "textarea" -> PreviewFieldType.TextArea;
            case "tags" -> PreviewFieldType.Tags;
            case "boolean" -> PreviewFieldType.Boolean;
            case "number" -> PreviewFieldType.Number;
            case "date" -> PreviewFieldType.Date;
            default -> PreviewFieldType.Text;
        };
    }

    private PreviewFieldValue extraFieldValue(String key, PreviewFieldType type, CkanDataset dataset) {
        PreviewFieldValue value = new PreviewFieldValue();
        value.setCode(key);
        Object raw = dataset.getCustomFields() != null ? dataset.getCustomFields().get(key) : null;
        if (raw == null) return value;
        switch (type) {
            case Tags -> {
                if (raw instanceof List<?> list) {
                    List<String> texts = new ArrayList<>();
                    for (Object entry : list) if (entry != null) texts.add(entry.toString());
                    value.setTextListValue(texts.isEmpty() ? null : texts);
                }
            }
            case Boolean -> { if (raw instanceof Boolean b) value.setBooleanValue(b); }
            case Number -> { if (raw instanceof Number n) value.setNumberValue(n.doubleValue()); }
            default -> value.setTextValue(raw.toString());
        }
        return value;
    }

    public void applyOverrides(CkanDataset dataset, List<PreviewFieldValue> editedFields, List<PreviewGroupValuesModel> editedGroups, DepositPluginSettings settings) {
        if (dataset == null) return;
        if (editedFields != null) {
            for (PreviewFieldValue value : editedFields) {
                if (value == null || value.getCode() == null || this.isLocked(value.getCode(), settings)) continue;
                this.applyFieldOverride(dataset, value);
            }
        }
        if (editedGroups != null) {
            for (PreviewGroupValuesModel group : editedGroups) {
                if (group == null || group.getGroupCode() == null) continue;
                switch (group.getGroupCode()) {
                    case GROUP_EXTRAS -> dataset.setExtras(this.buildExtrasFromItems(group.getItems()));
                    case GROUP_CREATORS -> {
                        if (this.hasIndexedCreatorFieldsConfigured()) this.applyCreatorsFromItems(dataset, group.getItems());
                    }
                    default -> {
                        // unknown group codes are ignored; they may come from a newer descriptor version
                    }
                }
            }
        }
    }

    /**
     * Writes the "creators" group rows back as the ordinal-indexed custom field keys CkanBuilder
     * expects (e.g. "creator_first_name_1", "creator_first_name_2", ...). Whole-list replacement:
     * any previously stored ordinal beyond the submitted row count is cleared so a shrunk list does
     * not leave stale trailing rows behind.
     */
    private void applyCreatorsFromItems(CkanDataset dataset, List<PreviewGroupItemModel> items) {
        this.clearIndexedCustomField(dataset, CUSTOM_FIELD_CREATOR_FIRST_NAME);
        this.clearIndexedCustomField(dataset, CUSTOM_FIELD_CREATOR_LAST_NAME);
        this.clearIndexedCustomField(dataset, CUSTOM_FIELD_CREATOR_ORCID);
        if (items == null || items.isEmpty()) return;

        int ordinal = 0;
        for (PreviewGroupItemModel item : items) {
            if (item == null || item.getValues() == null) continue;
            String givenName = this.itemText(item, ITEM_GIVEN_NAME);
            String familyName = this.itemText(item, ITEM_FAMILY_NAME);
            String orcid = this.itemText(item, ITEM_ORCID);
            if (givenName == null && familyName == null && orcid == null) continue;

            ordinal++;
            if (givenName != null) dataset.addCustomField(CUSTOM_FIELD_CREATOR_FIRST_NAME + "_" + ordinal, givenName);
            if (familyName != null) dataset.addCustomField(CUSTOM_FIELD_CREATOR_LAST_NAME + "_" + ordinal, familyName);
            if (orcid != null) dataset.addCustomField(CUSTOM_FIELD_CREATOR_ORCID + "_" + ordinal, orcid);
        }
    }

    private void clearIndexedCustomField(CkanDataset dataset, String key) {
        if (dataset.getCustomFields() == null) return;
        String prefix = key + "_";
        dataset.getCustomFields().keySet().removeIf(code -> code != null && code.startsWith(prefix));
    }

    private void applyFieldOverride(CkanDataset dataset, PreviewFieldValue value) {
        switch (value.getCode()) {
            case FIELD_TITLE -> { if (this.hasText(value)) dataset.setTitle(value.getTextValue()); }
            case FIELD_NOTES -> { if (this.hasText(value)) dataset.setNotes(value.getTextValue()); }
            case FIELD_VERSION -> { if (this.hasText(value)) dataset.setVersion(value.getTextValue()); }
            case FIELD_URL -> { if (this.hasText(value)) dataset.setUrl(value.getTextValue()); }
            case FIELD_TYPE -> { if (this.hasText(value)) dataset.setType(value.getTextValue()); }
            case FIELD_LICENSE -> { if (this.hasText(value)) dataset.setLicenseId(value.getTextValue()); }
            case FIELD_OWNER_ORG -> { if (this.hasText(value)) dataset.setOwnerOrg(value.getTextValue()); }
            case FIELD_PRIVATE -> { if (value.getBooleanValue() != null) dataset.setPrivate(value.getBooleanValue()); }
            case FIELD_AUTHOR -> { if (this.hasText(value)) dataset.setAuthor(value.getTextValue()); }
            case FIELD_AUTHOR_EMAIL -> { if (this.hasText(value)) dataset.setAuthorEmail(value.getTextValue()); }
            case FIELD_MAINTAINER -> { if (this.hasText(value)) dataset.setMaintainer(value.getTextValue()); }
            case FIELD_MAINTAINER_EMAIL -> { if (this.hasText(value)) dataset.setMaintainerEmail(value.getTextValue()); }
            case FIELD_TAGS -> {
                if (value.getTextListValue() != null) {
                    List<CkanTag> tags = new ArrayList<>();
                    for (String name : value.getTextListValue()) {
                        if (name == null || name.isBlank()) continue;
                        CkanTag tag = new CkanTag();
                        tag.setName(name);
                        tags.add(tag);
                    }
                    dataset.setTags(tags.isEmpty() ? null : tags);
                }
            }
            default -> this.applyExtraFieldOverride(dataset, value);
        }
    }

    private void applyExtraFieldOverride(CkanDataset dataset, PreviewFieldValue value) {
        // the indexed creator fields are rows of the dedicated "creators" group, never a plain scalar override
        if (INDEXED_CREATOR_CUSTOM_FIELD_CODES.contains(value.getCode())) return;

        // read-only and codes from a newer descriptor version fall through here and are ignored,
        // unless they match a configured deployment-specific extra field
        PreviewExtraFieldsProperties.ExtraField extraField = this.previewExtraFieldsProperties.getFields() != null
                ? this.previewExtraFieldsProperties.getFields().stream().filter(x -> x != null && value.getCode().equals(x.getKey())).findFirst().orElse(null)
                : null;
        if (extraField == null) return;

        Object converted = switch (this.extraFieldType(extraField.getType())) {
            case Tags -> {
                List<String> texts = value.getTextListValue();
                yield texts == null || texts.isEmpty() ? null : texts;
            }
            case Boolean -> value.getBooleanValue();
            case Number -> value.getNumberValue();
            default -> this.hasText(value) ? value.getTextValue() : null;
        };
        if (converted != null) dataset.addCustomField(extraField.getKey(), converted);
    }

    private List<CkanExtra> buildExtrasFromItems(List<PreviewGroupItemModel> items) {
        if (items == null || items.isEmpty()) return null;
        List<CkanExtra> extras = new ArrayList<>();
        for (PreviewGroupItemModel item : items) {
            if (item == null || item.getValues() == null) continue;
            String key = this.itemText(item, ITEM_EXTRA_KEY);
            if (key == null) continue;

            CkanExtra extra = new CkanExtra();
            extra.setKey(key);
            extra.setValue(this.itemText(item, ITEM_EXTRA_VALUE));
            extras.add(extra);
        }
        return extras.isEmpty() ? null : extras;
    }

    private String itemText(PreviewGroupItemModel item, String code) {
        for (PreviewFieldValue value : item.getValues()) {
            if (value != null && code.equals(value.getCode())) {
                return value.getTextValue() != null && !value.getTextValue().isBlank() ? value.getTextValue() : null;
            }
        }
        return null;
    }

    private boolean isLocked(String fieldCode, DepositPluginSettings settings) {
        return LOCKABLE_FIELDS.entrySet().stream().anyMatch(x -> x.getValue().equals(fieldCode) && settings.isLocked(x.getKey()));
    }

    private List<String> tagNames(CkanDataset dataset) {
        if (dataset.getTags() == null) return null;
        List<String> names = new ArrayList<>();
        for (CkanTag tag : dataset.getTags()) {
            if (tag != null && tag.getName() != null && !tag.getName().isBlank()) names.add(tag.getName());
        }
        return names;
    }

    private PreviewFieldGroup buildGroup(String code, String label) {
        PreviewFieldGroup group = new PreviewFieldGroup();
        group.setCode(code);
        group.setLabel(label);
        group.setFields(new ArrayList<>());
        return group;
    }

    private PreviewField buildField(String code, String label, PreviewFieldType type, PreviewFieldSource source, boolean required, java.util.function.Supplier<PreviewFieldValue> valueSupplier) {
        PreviewField field = new PreviewField();
        field.setCode(code);
        field.setLabel(label);
        field.setType(type);
        field.setSource(source);
        field.setRequired(required);
        field.setValue(valueSupplier.get());
        field.setBaseValue(valueSupplier.get());
        return field;
    }

    private PreviewField buildTemplateField(String code, String label, PreviewFieldType type) {
        PreviewField field = new PreviewField();
        field.setCode(code);
        field.setLabel(label);
        field.setType(type);
        field.setSource(PreviewFieldSource.Mapped);
        return field;
    }

    private PreviewFieldValue textValue(String code, String text) {
        PreviewFieldValue value = new PreviewFieldValue();
        value.setCode(code);
        value.setTextValue(text);
        return value;
    }

    private PreviewFieldValue textListValue(String code, List<String> texts) {
        PreviewFieldValue value = new PreviewFieldValue();
        value.setCode(code);
        value.setTextListValue(texts != null && !texts.isEmpty() ? texts : null);
        return value;
    }

    private PreviewFieldValue booleanValue(String code, Boolean bool) {
        PreviewFieldValue value = new PreviewFieldValue();
        value.setCode(code);
        value.setBooleanValue(bool);
        return value;
    }

    private boolean hasText(PreviewFieldValue value) {
        return value.getTextValue() != null && !value.getTextValue().isBlank();
    }
}
