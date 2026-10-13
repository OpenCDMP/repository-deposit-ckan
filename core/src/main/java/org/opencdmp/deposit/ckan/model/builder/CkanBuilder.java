package org.opencdmp.deposit.ckan.model.builder;

import gr.cite.tools.logging.LoggerService;
import gr.cite.tools.logging.MapLogEntry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opencdmp.commonmodels.enums.PlanAccessType;
import org.opencdmp.commonmodels.models.description.*;
import org.opencdmp.commonmodels.models.planblueprintdescriptiontemplatefields.fielddata.RadioBoxDataModel;
import org.opencdmp.commonmodels.models.planblueprintdescriptiontemplatefields.fielddata.SelectDataModel;
import org.opencdmp.commonmodels.models.plan.PlanContactModel;
import org.opencdmp.commonmodels.models.plan.PlanModel;
import org.opencdmp.commonmodels.models.plandescriptionfields.FieldModel;
import org.opencdmp.commonmodels.models.plandescriptionfields.PropertyDefinitionFieldSetItemModel;
import org.opencdmp.commonmodels.models.plandescriptionfields.PropertyDefinitionFieldSetModel;
import org.opencdmp.commonmodels.models.planreference.PlanReferenceModel;
import org.opencdmp.commonmodels.models.reference.ReferenceModel;
import org.opencdmp.deposit.ckan.configuration.SemanticsProperties;
import org.opencdmp.deposit.ckan.model.CkanDataset;
import org.opencdmp.deposit.ckan.model.CkanExtra;
import org.opencdmp.deposit.ckan.model.CkanGroup;
import org.opencdmp.deposit.ckan.model.CkanTag;
import org.opencdmp.deposit.ckan.model.DatasetRelationship;
import org.opencdmp.deposit.ckan.service.ckan.CkanDepositServiceImpl;
import org.opencdmp.deposit.ckan.service.ckan.CkanServiceProperties;
import org.opencdmp.deposit.ckan.service.ckan.LocalCustomFieldProperties;
import org.opencdmp.depositbase.enums.DataType;
import org.opencdmp.depositbase.repository.CustomFieldProperties;
import org.opencdmp.depositbase.repository.DepositPluginSettings;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CkanBuilder {
    private static final LoggerService logger = new LoggerService(LoggerFactory.getLogger(CkanDepositServiceImpl.class));
    private static final Log log = LogFactory.getLog(CkanBuilder.class);

    private static final String SEMANTIC_DATASET_ORGANIZATION = "ckan.dataset.organization";
    private static final String SEMANTIC_DATASET_TAGS = "ckan.dataset.tags";
    private static final String SEMANTIC_DATASET_EXTRAS = "ckan.dataset.extras";
    private static final String SEMANTIC_DATASET_LICENSE = "ckan.dataset.license";
    private static final String SEMANTIC_DATASET_SOURCE = "ckan.dataset.source";
    private static final String SEMANTIC_DATASET_GROUPS = "ckan.dataset.groups";
    private static final String SEMANTIC_CUSTOM_FIELD_PREFIX = "ckan.custom_fields.";
    public static final String SETTING_ORGANISATION = "organisation";
    public static final String SETTING_VISIBILITY = "visibility";
    public static final String SETTING_LICENCE = "licence";
    public static final String SETTING_MAINTAINER = "maintainer";
    public static final String SETTING_MAINTAINER_EMAIL = "maintainer-email";
    private static final String SETTING_TAGS = "tags";
    private static final String SETTING_GROUPS = "groups";
    private static final String SETTING_DATASET_TYPE = "dataset-type";
    private static final String SETTING_REFERENCE_TYPE_ORGANISATIONS = "reference-type-organisations";
    private static final String SETTING_REFERENCE_TYPE_LICENCES = "reference-type-licences";
    private static final String VISIBILITY_PRIVATE = "private";
    private static final String VISIBILITY_PUBLIC = "public";

    // custom fields that must be deposited as multiple, per-creator CKAN keys (e.g. "creator_first_name_1", "creator_first_name_2", ...)
    private static final Set<String> INDEXED_CUSTOM_FIELD_CODES = Set.of("creator_first_name", "creator_last_name", "creator_orcid_id");

    private final CkanServiceProperties ckanServiceProperties;
    private final LocalCustomFieldProperties customFieldProperties;
    private final SemanticsProperties semanticsProperties;

    @Autowired
    public CkanBuilder(CkanServiceProperties ckanServiceProperties, LocalCustomFieldProperties customFieldProperties, SemanticsProperties semanticsProperties){
            this.ckanServiceProperties = ckanServiceProperties;
        this.customFieldProperties = customFieldProperties;
        this.semanticsProperties = semanticsProperties;
    }

    public CkanDataset build(PlanModel planModel) {
        CkanDataset dataset = new CkanDataset();

        if (planModel == null) return dataset;

        DepositPluginSettings settings = DepositPluginSettings.of(planModel, this.ckanServiceProperties.getDepositConfiguration());

        dataset.setTitle(planModel.getLabel());
        dataset.setName(planModel.getId().toString());
        String visibility = settings.text(SETTING_VISIBILITY);
        if (VISIBILITY_PRIVATE.equals(visibility)) dataset.setPrivate(true);
        else if (VISIBILITY_PUBLIC.equals(visibility)) dataset.setPrivate(false);
        else dataset.setPrivate(planModel.getAccessType().equals(PlanAccessType.Restricted));
        dataset.setNotes(planModel.getDescription());
        dataset.setVersion(String.valueOf(planModel.getVersion()));
        dataset.setType(settings.text(SETTING_DATASET_TYPE));

        String url = this.applySingleValue(planModel, SEMANTIC_DATASET_SOURCE);
        if (url == null && planModel.getAccessType().equals(PlanAccessType.Public) && settings.linkPublicPlan()) dataset.setUrl(this.ckanServiceProperties.getDomain() + "explore-plans/overview/public/" + planModel.getId().toString());
        else dataset.setUrl(url);

        this.applyOwnerOrg(planModel, dataset, settings);
        this.applyTags(planModel, dataset, settings);
        this.applyGroups(planModel, dataset, settings);
        this.applyCreator(planModel, dataset);
        this.applyContacts(planModel, dataset, settings);
        this.applyLicenses(planModel, dataset, settings);
        this.applyExtras(planModel, dataset);
        this.applyCustomFields(planModel, dataset);

        return dataset;
    }

    public List<DatasetRelationship> buildDatasetRelationshipBySemantics(PlanModel planModel, String datasetId) {
        List<DatasetRelationship> relationships = new ArrayList<>();

        if (planModel == null || datasetId == null) return relationships;

        for (String semantic: semanticsProperties.getRelationshipsAsObject() != null ? semanticsProperties.getRelationshipsAsObject() : List.<String>of()) {
            this.buildDatasetRelationshipBySemantics(planModel, relationships, semantic, datasetId,true);
        }

        for (String semantic: semanticsProperties.getRelationshipsAsSubject() != null ? semanticsProperties.getRelationshipsAsSubject() : List.<String>of()) {
            this.buildDatasetRelationshipBySemantics(planModel, relationships, semantic, datasetId,false);
        }

        return relationships;
    }

    private void buildDatasetRelationshipBySemantics(PlanModel planModel, List<DatasetRelationship> relationships, String semantic, String datasetId, boolean isRelationshipAsObject) {
        //plan blueprint semantics
        List<org.opencdmp.commonmodels.models.planblueprintdescriptiontemplatefields.FieldModel> fieldsWithSemantics = this.findSchematicValues(semantic, planModel.getPlanBlueprint().getDefinition().getAllField());
        Set<String> values = extractSchematicValues(fieldsWithSemantics, planModel.getProperties().getFieldSets());
        this.buildRelationships(values, relationships, semantic, datasetId, isRelationshipAsObject);

        //description template semantics
        if (planModel.getDescriptions() == null) return;
        for (DescriptionModel descriptionModel: planModel.getDescriptions()) {
            fieldsWithSemantics = this.findSchematicValues(semantic, descriptionModel.getDescriptionTemplate().getDefinition().getAllField());
            values = extractSchematicValues(fieldsWithSemantics, descriptionModel.getProperties().getFieldSets());
            this.buildRelationships(values, relationships, semantic, datasetId, isRelationshipAsObject);
        }
    }

    private void buildRelationships(Set<String> values, List<DatasetRelationship> relationships, String semantic, String datasetId, boolean isRelationshipAsObject) {
        for (String value: values) {
            DatasetRelationship relationship = new DatasetRelationship();
            relationship.setType(semantic.substring(semantic.lastIndexOf(".") + 1));
            if (isRelationshipAsObject) {
                relationship.setObject(value);
                relationship.setSubject(datasetId);
            } else {
                relationship.setObject(datasetId);
                relationship.setSubject(value);
            }
            if (!relationships.contains(relationship)) relationships.add(relationship);
        }
    }

    private void applyCreator(PlanModel planModel, CkanDataset dataset) {
        if (planModel.getCreator() != null) {
            dataset.setAuthor(planModel.getCreator().getName());
            if (planModel.getCreator().getContacts() != null && !planModel.getCreator().getContacts().isEmpty()) dataset.setAuthorEmail(planModel.getCreator().getContacts().getFirst().getValue());
        }
    }

    private void applyContacts(PlanModel planModel, CkanDataset dataset, DepositPluginSettings settings) {
        if (planModel.getProperties() != null && planModel.getProperties().getContacts() != null && !planModel.getProperties().getContacts().isEmpty()) {
            StringBuilder maintainerString = new StringBuilder();

            if (planModel.getProperties().getContacts().size() == 1) {
                PlanContactModel planContactModel = planModel.getProperties().getContacts().getFirst();
                maintainerString.append(planContactModel.getFirstName()).append(" ").append(planContactModel.getLastName());
                dataset.setMaintainerEmail(planContactModel.getEmail());
            } else {
                int i = 0;
                for (PlanContactModel planContactModel : planModel.getProperties().getContacts()) {
                    i++;
                    if (planContactModel != null){
                        maintainerString.append(planContactModel.getFirstName()).append(" ").append(planContactModel.getLastName()).append(" (").append(planContactModel.getEmail()).append(") ").append(i < planModel.getProperties().getContacts().size() ? ", " : "");
                    }
                }
            }

            dataset.setMaintainer(maintainerString.toString());
        } else {
            dataset.setMaintainer(settings.text(SETTING_MAINTAINER));
            dataset.setMaintainerEmail(settings.text(SETTING_MAINTAINER_EMAIL));
        }
    }

    private void applyLicenses(PlanModel planModel, CkanDataset dataset, DepositPluginSettings settings) {
        List<String> licenseIds = this.getLicenseIds();

        if (licenseIds != null && !licenseIds.isEmpty()) {
            String license = this.applySingleValue(planModel, SEMANTIC_DATASET_LICENSE);
            if (license == null) {
                List<ReferenceModel> planLicenses = this.getReferenceModelOfType(planModel, settings.text(SETTING_REFERENCE_TYPE_LICENCES, ckanServiceProperties.getLicenseReferenceCode()));
                if (!planLicenses.isEmpty()) {
                    for (ReferenceModel planLicense : planLicenses) {
                        if (planLicense != null && planLicense.getReference() != null && !planLicense.getReference().isBlank()) {
                            if (licenseIds.contains(planLicense.getReference())) {
                                dataset.setLicenseId(planLicense.getReference());
                                break;
                            }
                        }
                    }
                }
            } else {
                if (licenseIds.contains(license)) {
                    dataset.setLicenseId(license);
                }
            }

            String fallback = settings.text(SETTING_LICENCE);
            if (dataset.getLicenseId() == null && fallback != null && licenseIds.contains(fallback)) dataset.setLicenseId(fallback);
        }
    }

    private void applyOwnerOrg(PlanModel planModel, CkanDataset dataset, DepositPluginSettings settings) {
        List<String> organizationIds = this.getOrganizationIds();

        if (organizationIds != null && !organizationIds.isEmpty()) {
            String organization = this.applySingleValue(planModel, SEMANTIC_DATASET_ORGANIZATION);
            if (organization == null) {
                List<ReferenceModel> planOrganizations = this.getReferenceModelOfType(planModel, settings.text(SETTING_REFERENCE_TYPE_ORGANISATIONS, ckanServiceProperties.getOrganizationReferenceCode()));
                if (!planOrganizations.isEmpty()) {
                    for (ReferenceModel planOrganization : planOrganizations) {
                        if (planOrganization != null && planOrganization.getReference() != null && !planOrganization.getReference().isBlank()) {
                            if (organizationIds.contains(planOrganization.getReference())) {
                                dataset.setOwnerOrg(planOrganization.getReference());
                                break;
                            }
                        }
                    }
                }
            } else {
                if (organizationIds.contains(organization)) {
                    dataset.setOwnerOrg(organization);
                }
            }
        }

        //fallback
        if (dataset.getOwnerOrg() == null) dataset.setOwnerOrg(settings.text(SETTING_ORGANISATION, this.ckanServiceProperties.getOrganization()));
    }

    private void applyGroups(PlanModel planModel, CkanDataset dataset, DepositPluginSettings settings) {
        Set<String> names = new LinkedHashSet<>(this.applyListValue(planModel, SEMANTIC_DATASET_GROUPS));
        names.addAll(settings.list(SETTING_GROUPS));

        List<CkanGroup> groups = new ArrayList<>();
        for (String name : names) {
            if (name == null || name.isBlank()) continue;
            CkanGroup group = new CkanGroup();
            group.setName(name.trim());
            groups.add(group);
        }
        if (!groups.isEmpty()) dataset.setGroups(groups);
    }

    private void applyTags(PlanModel planModel, CkanDataset dataset, DepositPluginSettings settings) {
        List<CkanTag> tags = new ArrayList<>();

        //plan blueprint semantics
        List<org.opencdmp.commonmodels.models.planblueprintdescriptiontemplatefields.FieldModel> fieldsWithSemantics = this.findSchematicValues(SEMANTIC_DATASET_TAGS, planModel.getPlanBlueprint().getDefinition().getAllField());
        Set<String> values = extractSchematicValues(fieldsWithSemantics, planModel.getProperties().getFieldSets());
        //description tags from semantic
        for (String value: values){
            CkanTag tag = new CkanTag();
            tag.setName(value);
            if (isValidTag(value) && !tags.contains(tag)) tags.add(tag);
        }

        //description template
        for (DescriptionModel descriptionModel: planModel.getDescriptions() != null ? planModel.getDescriptions() : List.<DescriptionModel>of()) {
            fieldsWithSemantics = this.findSchematicValues(SEMANTIC_DATASET_TAGS, descriptionModel.getDescriptionTemplate().getDefinition().getAllField());
            values = extractSchematicValues(fieldsWithSemantics, descriptionModel.getProperties().getFieldSets());
            //description tags from semantic
            for (String value: values){
                CkanTag tag = new CkanTag();
                tag.setName(value);
                if (isValidTag(value) && !tags.contains(tag)) tags.add(tag);
            }

            if (descriptionModel.getTags() != null) {
                //description tags
                for (String descriptionTag: descriptionModel.getTags()) {
                    CkanTag tag = new CkanTag();
                    tag.setName(descriptionTag);
                    if (isValidTag(descriptionTag) && !tags.contains(tag)) tags.add(tag);
                }
            }

        }

        for (String settingTag : settings.list(SETTING_TAGS)) {
            CkanTag tag = new CkanTag();
            tag.setName(settingTag);
            if (isValidTag(settingTag) && tags.stream().noneMatch(x -> settingTag.equals(x.getName()))) tags.add(tag);
        }

        if (!tags.isEmpty()) dataset.setTags(tags);
    }

    private boolean isValidTag(String tag) {
        return Pattern.compile("^[a-zA-Z0-9 ._-]+$").matcher(tag).matches();
    }

    private void applyExtras(PlanModel planModel, CkanDataset dataset) {

        // CKAN rejects a dataset whose extras repeat a key, so a field answered in several places becomes one extra with its distinct values joined
        Map<String, Set<String>> extraValues = new LinkedHashMap<>();

        //plan blueprint semantics
        List<org.opencdmp.commonmodels.models.planblueprintdescriptiontemplatefields.FieldModel> fieldsWithSemantics = this.findSchematicValues(SEMANTIC_DATASET_EXTRAS, planModel.getPlanBlueprint().getDefinition().getAllField());
        for (org.opencdmp.commonmodels.models.planblueprintdescriptiontemplatefields.FieldModel field : fieldsWithSemantics) {
            if (field.getData() == null) continue;
            List<org.opencdmp.commonmodels.models.plandescriptionfields.FieldModel> valueFields = this.findValueFieldsByIds(field.getId(),  planModel.getProperties().getFieldSets());
            for (org.opencdmp.commonmodels.models.plandescriptionfields.FieldModel valueField : valueFields) {
                String value = this.extractSchematicSingleValue(List.of(field), planModel.getProperties().getFieldSets());
                if (value != null) extraValues.computeIfAbsent(valueField.getId(), k -> new LinkedHashSet<>()).add(value);
            }
        }

        //description template semantics
        for (DescriptionModel descriptionModel: planModel.getDescriptions() != null ? planModel.getDescriptions() : List.<DescriptionModel>of()) {
            fieldsWithSemantics = this.findSchematicValues(SEMANTIC_DATASET_EXTRAS, descriptionModel.getDescriptionTemplate().getDefinition().getAllField());
            for (org.opencdmp.commonmodels.models.planblueprintdescriptiontemplatefields.FieldModel field : fieldsWithSemantics) {
                if (field.getData() == null) continue;
                List<org.opencdmp.commonmodels.models.plandescriptionfields.FieldModel> valueFields = this.findValueFieldsByIds(field.getId(),  descriptionModel.getProperties().getFieldSets());
                for (org.opencdmp.commonmodels.models.plandescriptionfields.FieldModel valueField : valueFields) {
                    String value = this.extractSchematicSingleValue(List.of(field), descriptionModel.getProperties().getFieldSets());
                    if (value != null) extraValues.computeIfAbsent(valueField.getId(), k -> new LinkedHashSet<>()).add(value);
                }
            }
        }

        List<CkanExtra> extras = new ArrayList<>();
        for (Map.Entry<String, Set<String>> entry : extraValues.entrySet()) {
            CkanExtra extra = new CkanExtra();
            extra.setKey(entry.getKey());
            extra.setValue(String.join(", ", entry.getValue()));
            extras.add(extra);
        }
        if (!extras.isEmpty()) dataset.setExtras(extras);
    }

    private List<ReferenceModel> getReferenceModelOfType(PlanModel planModel, String code){
        List<ReferenceModel> response = new ArrayList<>();
        if (planModel.getReferences() == null) return response;
        for (PlanReferenceModel planReferenceModel : planModel.getReferences()){
            if (planReferenceModel.getReference() != null && planReferenceModel.getReference().getType() != null && planReferenceModel.getReference().getType().getCode() != null  && planReferenceModel.getReference().getType().getCode().equals(code)){
                response.add(planReferenceModel.getReference());
            }
        }
        return response;
    }

    private List<PlanReferenceModel> getPlanReferenceModelOfType(PlanModel planModel, String code){
        List<PlanReferenceModel> response = new ArrayList<>();
        if (planModel.getReferences() == null) return response;
        for (PlanReferenceModel planReferenceModel : planModel.getReferences()){
            if (planReferenceModel.getReference() != null && planReferenceModel.getReference().getType() != null && planReferenceModel.getReference().getType().getCode() != null  && planReferenceModel.getReference().getType().getCode().equals(code)){
                response.add(planReferenceModel);
            }
        }
        return response;
    }


    private String applySingleValue(PlanModel planModel, String semanticCode) {

        List<String> value = this.applyListValue(planModel, semanticCode);

        return !value.isEmpty()? value.getFirst(): null;
    }

    private List<String> applyListValue(PlanModel planModel, String semanticCode) {
        List<String> fields = new ArrayList<>();

        //plan blueprint semantics
        List<org.opencdmp.commonmodels.models.planblueprintdescriptiontemplatefields.FieldModel> fieldsWithSemantics = this.findSchematicValues(semanticCode, planModel.getPlanBlueprint().getDefinition().getAllField());
        Set<String> values = extractSchematicValues(fieldsWithSemantics, planModel.getProperties().getFieldSets());

        if (!values.isEmpty()) {
            fields.addAll(values.stream().toList());
        }

        if (planModel.getDescriptions() != null) {
            //description template
            for (DescriptionModel descriptionModel: planModel.getDescriptions()) {
                fieldsWithSemantics = this.findSchematicValues(semanticCode, descriptionModel.getDescriptionTemplate().getDefinition().getAllField());
                values = extractSchematicValues(fieldsWithSemantics, descriptionModel.getProperties().getFieldSets());
                if (!values.isEmpty()) {
                    fields.addAll(values.stream().toList());
                }
            }
        }

        return fields;
    }

    private List<org.opencdmp.commonmodels.models.planblueprintdescriptiontemplatefields.FieldModel> findSchematicValues(String relatedId, List<org.opencdmp.commonmodels.models.planblueprintdescriptiontemplatefields.FieldModel> fields){
        return fields.stream().filter(x-> x.getSemantics() != null && x.getSemantics().contains(relatedId)).toList();
    }

    private String extractSchematicSingleValue(List<org.opencdmp.commonmodels.models.planblueprintdescriptiontemplatefields.FieldModel> fields, Map<String, PropertyDefinitionFieldSetModel> fieldSets) {
        Set<String> value = this.extractSchematicValues(fields, fieldSets);
        return !value.isEmpty() ? value.stream().findFirst().orElse(null): null;
    }

    private Set<String> extractSchematicValues(List<org.opencdmp.commonmodels.models.planblueprintdescriptiontemplatefields.FieldModel> fields, Map<String, PropertyDefinitionFieldSetModel> fieldSets) {
        Set<String> values = new HashSet<>();
        for (org.opencdmp.commonmodels.models.planblueprintdescriptiontemplatefields.FieldModel field : fields) {
            if (field.getData() == null) continue;
            List<org.opencdmp.commonmodels.models.plandescriptionfields.FieldModel> valueFields = this.findValueFieldsByIds(field.getId(), fieldSets);
            for (org.opencdmp.commonmodels.models.plandescriptionfields.FieldModel valueField : valueFields) {
                switch (field.getData().getFieldType()) {
                    case FREE_TEXT, TEXT_AREA, RICH_TEXT_AREA -> {
                        if (valueField.getTextValue() != null && !valueField.getTextValue().isBlank()) values.add(valueField.getTextValue());
                    }
                    case NUMBER -> {
                        if (valueField.getNumberValue() != null) values.add(String.valueOf(valueField.getNumberValue()));
                    }
                    case BOOLEAN_DECISION, CHECK_BOX -> {
                        if (valueField.getBooleanValue() != null) values.add(valueField.getBooleanValue().toString());
                    }
                    case DATE_PICKER -> {
                        try {
                            if (valueField.getDateValue() != null) values.add((DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault()).format(valueField.getDateValue())));
                        } catch (Exception e) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                    case DATASET_IDENTIFIER, VALIDATION -> {
                        if (valueField.getExternalIdentifier() != null && valueField.getExternalIdentifier().getIdentifier() != null && !valueField.getExternalIdentifier().getIdentifier().isBlank()) {
                            values.add(valueField.getExternalIdentifier().getIdentifier());
                        }
                    }
                    case TAGS -> {
                        if (valueField.getTextListValue() != null && !valueField.getTextListValue().isEmpty()) {
                            values.addAll(valueField.getTextListValue());
                        }
                    }
                    case SELECT -> {
                        if (valueField.getTextListValue() != null && !valueField.getTextListValue().isEmpty()) {
                            SelectDataModel selectDataModel = (SelectDataModel)field.getData();
                            if (selectDataModel != null && selectDataModel.getOptions() != null && !selectDataModel.getOptions().isEmpty()){
                                for (SelectDataModel.OptionModel option : selectDataModel.getOptions()){
                                    if (valueField.getTextListValue().contains(option.getValue()) || valueField.getTextListValue().contains(option.getLabel())) values.add(option.getLabel());
                                }
                            }
                        }
                    }
                    case RADIO_BOX -> {
                        if (valueField.getTextListValue() != null && !valueField.getTextListValue().isEmpty()) {
                            RadioBoxDataModel radioBoxModel = (RadioBoxDataModel)field.getData();
                            if (radioBoxModel != null && radioBoxModel.getOptions() != null && !radioBoxModel.getOptions().isEmpty()){
                                for (RadioBoxDataModel.RadioBoxOptionModel option : radioBoxModel.getOptions()){
                                    if (valueField.getTextListValue().contains(option.getValue()) || valueField.getTextListValue().contains(option.getLabel())) values.add(option.getLabel());
                                }
                            }
                        }
                    }
                    case REFERENCE_TYPES -> {
                        if (valueField.getReferences() != null && !valueField.getReferences().isEmpty()) {
                            for (ReferenceModel referenceModel : valueField.getReferences()) {
                                if (referenceModel == null
                                        || referenceModel.getType() == null || referenceModel.getType().getCode() == null || referenceModel.getType().getCode().isBlank()
                                        || referenceModel.getDefinition() == null || referenceModel.getDefinition().getFields() == null || referenceModel.getDefinition().getFields().isEmpty()) continue;
                                if (referenceModel.getReference() != null && !referenceModel.getReference().isBlank()) {
                                    values.add(referenceModel.getReference());
                                }
                            }
                        }
                    }
                }
            }
        }
        return values;
    }

    private List<org.opencdmp.commonmodels.models.plandescriptionfields.FieldModel> findValueFieldsByIds(String fieldId, Map<String, PropertyDefinitionFieldSetModel> fieldSets){
        List<org.opencdmp.commonmodels.models.plandescriptionfields.FieldModel> models = new ArrayList<>();
        if (fieldSets == null || fieldSets.isEmpty()) return models;
        for (PropertyDefinitionFieldSetModel propertyDefinitionFieldSetModel : fieldSets.values()){
            if (propertyDefinitionFieldSetModel == null ||propertyDefinitionFieldSetModel.getItems() == null || propertyDefinitionFieldSetModel.getItems().isEmpty()) continue;
            for (PropertyDefinitionFieldSetItemModel propertyDefinitionFieldSetItemModel : propertyDefinitionFieldSetModel.getItems()){
                if (propertyDefinitionFieldSetItemModel == null ||propertyDefinitionFieldSetItemModel.getFields() == null || propertyDefinitionFieldSetItemModel.getFields().isEmpty()) continue;
                for (Map.Entry<String, org.opencdmp.commonmodels.models.plandescriptionfields.FieldModel> entry : propertyDefinitionFieldSetItemModel.getFields().entrySet()){
                    if (entry == null || entry.getValue() == null) continue;
                    if (entry.getKey().equalsIgnoreCase(fieldId)) models.add(entry.getValue());
                }
            }
        }
        return models;
    }

    private List<String> getLicenseIds(){

        try {
            String url = this.ckanServiceProperties.getDepositConfiguration().getRepositoryUrl() + "license_list";
            Map<String, Object> response = this.getWebClient().get().uri(url).retrieve().toEntity(Map.class).block().getBody();
            if (response == null) return null;
            List<Map<String, String>> licenses = (List<Map<String, String>>) response.get("result");

            return licenses.stream()
                    .map(map -> map.get("id"))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return null;
    }

    private List<String> getOrganizationIds(){

        try {
            String url = this.ckanServiceProperties.getDepositConfiguration().getRepositoryUrl() + "organization_list";
            Map<String, Object> response = this.getWebClient().get().uri(url).retrieve().toEntity(Map.class).block().getBody();
            if (response == null) return null;
            return (List<String>) response.get("result");

        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return null;
    }

    private void applyCustomFields(PlanModel planModel, CkanDataset dataset) {
        if (planModel == null || dataset == null) return;
        if (this.customFieldProperties.getCustomFields() == null || this.customFieldProperties.getCustomFields().isEmpty()) return;

        for (CustomFieldProperties.CustomField customField : this.customFieldProperties.getCustomFields()) {
            if (customField == null || customField.getCode() == null || customField.getCode().isBlank()) continue;

            String semantic = SEMANTIC_CUSTOM_FIELD_PREFIX + customField.getCode();

            if (INDEXED_CUSTOM_FIELD_CODES.contains(customField.getCode())) {
                applyIndexedCustomField(planModel, dataset, customField, semantic);
                continue;
            }

            Set<String> rawValues = extractValuesBySemantic(planModel, semantic);
            if (rawValues.isEmpty()) continue;

            Object mappedValue = mapCustomFieldValue(customField, rawValues);
            if (mappedValue == null) continue;

            String jPath = (customField.getjPath() != null && !customField.getjPath().isBlank())
                    ? customField.getjPath()
                    : customField.getCode();

            setNestedCustomField(dataset, jPath, mappedValue);
        }
    }

    // deposits one CKAN key per repeatable-fieldset instance, e.g. "creator_first_name_1", "creator_first_name_2", ...
    // the "_n" suffix is the 1-based ordinal of the fieldset item (row) the value came from, so that first/last name/orcid of the
    // same creator instance share the same suffix.
    private void applyIndexedCustomField(PlanModel planModel, CkanDataset dataset, CustomFieldProperties.CustomField customField, String semantic) {
        Map<Integer, String> valuesByOrdinal = extractValuesBySemanticIndexed(planModel, semantic);
        if (valuesByOrdinal.isEmpty()) return;

        String jPathBase = (customField.getjPath() != null && !customField.getjPath().isBlank())
                ? customField.getjPath()
                : customField.getCode();

        for (Map.Entry<Integer, String> entry : valuesByOrdinal.entrySet()) {
            Object mappedValue = convertCustomFieldValue(entry.getValue(), customField.getDataType());
            if (mappedValue == null) continue;

            setNestedCustomField(dataset, jPathBase + "_" + entry.getKey(), mappedValue);
        }
    }

    @SuppressWarnings("unchecked")
    private void setNestedCustomField(CkanDataset dataset, String jPath, Object value) {
        String[] segments = jPath.split("\\.");

        if (segments.length == 1) {
            dataset.addCustomField(segments[0], value);
            return;
        }

        // walk/build the nested map structure inside customFields
        Map<String, Object> customFields = dataset.getCustomFields();
        Map<String, Object> current = customFields;

        for (int i = 0; i < segments.length - 1; i++) {
            String segment = segments[i];
            Object existing = current.get(segment);
            if (existing instanceof Map) {
                current = (Map<String, Object>) existing;
            } else {
                Map<String, Object> nested = new LinkedHashMap<>();
                current.put(segment, nested);
                current = nested;
            }
        }

        current.put(segments[segments.length - 1], value);
    }

    private Set<String> extractValuesBySemantic(PlanModel planModel, String semanticCode) {
        Set<String> values = new LinkedHashSet<>();
        if (planModel == null || semanticCode == null || semanticCode.isBlank()) return values;

        // plan blueprint semantics
        List<org.opencdmp.commonmodels.models.planblueprintdescriptiontemplatefields.FieldModel> planFieldsWithSemantics = this.findSchematicValues(semanticCode, planModel.getPlanBlueprint().getDefinition().getAllField());
        if (planFieldsWithSemantics != null && !planFieldsWithSemantics.isEmpty()) {
            values.addAll(extractSchematicValues(planFieldsWithSemantics, planModel.getProperties().getFieldSets()));
        }

        // description template semantics
        if (planModel.getDescriptions() != null) {
            for (DescriptionModel descriptionModel : planModel.getDescriptions()) {
                List<org.opencdmp.commonmodels.models.planblueprintdescriptiontemplatefields.FieldModel> fieldsWithSemantics = this.findSchematicValues(semanticCode, descriptionModel.getDescriptionTemplate().getDefinition().getAllField());
                if (fieldsWithSemantics != null && !fieldsWithSemantics.isEmpty()) {
                    values.addAll(extractSchematicValues(fieldsWithSemantics, descriptionModel.getProperties().getFieldSets()));
                }

            }
        }

        return values;
    }

    // like extractValuesBySemantic, but keyed by the 1-based ordinal of the repeatable fieldset item (row) the value came from,
    // instead of collapsing all matches into a flat set. Only description template fieldsets carry this per-row structure;
    // plan blueprint values have no repeatable/ordinal concept and are ignored here.
    private Map<Integer, String> extractValuesBySemanticIndexed(PlanModel planModel, String semanticCode) {
        Map<Integer, String> valuesByOrdinal = new LinkedHashMap<>();
        if (planModel == null || semanticCode == null || semanticCode.isBlank() || planModel.getDescriptions() == null) return valuesByOrdinal;

        for (DescriptionModel descriptionModel : planModel.getDescriptions()) {
            List<org.opencdmp.commonmodels.models.planblueprintdescriptiontemplatefields.FieldModel> fieldsWithSemantics =
                    this.findSchematicValues(semanticCode, descriptionModel.getDescriptionTemplate().getDefinition().getAllField());

            for (org.opencdmp.commonmodels.models.planblueprintdescriptiontemplatefields.FieldModel field : fieldsWithSemantics) {
                if (field.getData() == null) continue;
                Map<Integer, FieldModel> valueFieldsByOrdinal = this.findValueFieldsByIdsIndexed(field.getId(), descriptionModel.getProperties());
                for (Map.Entry<Integer, FieldModel> entry : valueFieldsByOrdinal.entrySet()) {
                    String value = this.extractSingleFieldValue(field, entry.getValue());
                    if (value != null && !valuesByOrdinal.containsKey(entry.getKey())) valuesByOrdinal.put(entry.getKey(), value);
                }
            }
        }

        return valuesByOrdinal;
    }

    private String extractSingleFieldValue(org.opencdmp.commonmodels.models.planblueprintdescriptiontemplatefields.FieldModel field, FieldModel valueField) {
        return switch (field.getData().getFieldType()) {
            case FREE_TEXT, TEXT_AREA, RICH_TEXT_AREA ->
                    (valueField.getTextValue() != null && !valueField.getTextValue().isBlank()) ? valueField.getTextValue() : null;
            case BOOLEAN_DECISION, CHECK_BOX ->
                    valueField.getBooleanValue() != null ? valueField.getBooleanValue().toString() : null;
            case DATE_PICKER -> {
                try {
                    yield valueField.getDateValue() != null
                            ? DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault()).format(valueField.getDateValue())
                            : null;
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    yield null;
                }
            }
            case DATASET_IDENTIFIER, VALIDATION ->
                    (valueField.getExternalIdentifier() != null && valueField.getExternalIdentifier().getIdentifier() != null && !valueField.getExternalIdentifier().getIdentifier().isBlank())
                            ? valueField.getExternalIdentifier().getIdentifier() : null;
            case TAGS ->
                    (valueField.getTextListValue() != null && !valueField.getTextListValue().isEmpty())
                            ? String.join(", ", valueField.getTextListValue()) : null;
            case SELECT -> {
                if (valueField.getTextListValue() == null || valueField.getTextListValue().isEmpty()) yield null;
                SelectDataModel selectDataModel = (SelectDataModel) field.getData();
                if (selectDataModel == null || selectDataModel.getOptions() == null || selectDataModel.getOptions().isEmpty()) yield null;
                List<String> values = new ArrayList<>();
                for (SelectDataModel.OptionModel option : selectDataModel.getOptions()) {
                    if (valueField.getTextListValue().contains(option.getValue()) || valueField.getTextListValue().contains(option.getLabel())) values.add(option.getLabel());
                }
                yield values.isEmpty() ? null : String.join(", ", values);
            }
            case RADIO_BOX -> {
                if (valueField.getTextListValue() == null || valueField.getTextListValue().isEmpty()) yield null;
                RadioBoxDataModel radioBoxModel = (RadioBoxDataModel) field.getData();
                if (radioBoxModel == null || radioBoxModel.getOptions() == null || radioBoxModel.getOptions().isEmpty()) yield null;
                List<String> values = new ArrayList<>();
                for (RadioBoxDataModel.RadioBoxOptionModel option : radioBoxModel.getOptions()) {
                    if (valueField.getTextListValue().contains(option.getValue()) || valueField.getTextListValue().contains(option.getLabel())) values.add(option.getLabel());
                }
                yield values.isEmpty() ? null : String.join(", ", values);
            }
            case REFERENCE_TYPES -> {
                if (valueField.getReferences() == null || valueField.getReferences().isEmpty()) yield null;
                List<String> referenceLabels = valueField.getReferences().stream()
                        .filter(referenceModel -> referenceModel != null
                                && referenceModel.getType() != null && referenceModel.getType().getCode() != null && !referenceModel.getType().getCode().isBlank()
                                && referenceModel.getDefinition() != null && referenceModel.getDefinition().getFields() != null && !referenceModel.getDefinition().getFields().isEmpty())
                        .map(ReferenceModel::getLabel)
                        .toList();
                yield referenceLabels.isEmpty() ? null : String.join(", ", referenceLabels);
            }
            default -> null;
        };
    }

    // like findValueFieldsByIds, but keyed by the 1-based ordinal of the fieldset item (row) each value field belongs to
    private Map<Integer, FieldModel> findValueFieldsByIdsIndexed(String fieldId, PropertyDefinitionModel definitionModel) {
        Map<Integer, FieldModel> modelsByOrdinal = new LinkedHashMap<>();
        if (definitionModel == null || definitionModel.getFieldSets() == null || definitionModel.getFieldSets().isEmpty()) return modelsByOrdinal;

        for (PropertyDefinitionFieldSetModel propertyDefinitionFieldSetModel : definitionModel.getFieldSets().values()) {
            if (propertyDefinitionFieldSetModel == null || propertyDefinitionFieldSetModel.getItems() == null || propertyDefinitionFieldSetModel.getItems().isEmpty()) continue;
            for (PropertyDefinitionFieldSetItemModel propertyDefinitionFieldSetItemModel : propertyDefinitionFieldSetModel.getItems()) {
                if (propertyDefinitionFieldSetItemModel == null || propertyDefinitionFieldSetItemModel.getFields() == null || propertyDefinitionFieldSetItemModel.getFields().isEmpty()) continue;
                for (Map.Entry<String, FieldModel> entry : propertyDefinitionFieldSetItemModel.getFields().entrySet()) {
                    if (entry == null || entry.getValue() == null) continue;
                    if (entry.getKey().equalsIgnoreCase(fieldId)) {
                        // fieldset item ordinal is 0-based; CKAN keys must start at _1
                        int ordinal = propertyDefinitionFieldSetItemModel.getOrdinal() != null ? propertyDefinitionFieldSetItemModel.getOrdinal() + 1 : modelsByOrdinal.size() + 1;
                        modelsByOrdinal.put(ordinal, entry.getValue());
                    }
                }
            }
        }
        return modelsByOrdinal;
    }

    private Object mapCustomFieldValue(CustomFieldProperties.CustomField customField, Set<String> rawValues) {
        if (rawValues == null || rawValues.isEmpty()) return null;

        List<Object> converted = rawValues.stream()
                .map(value -> convertCustomFieldValue(value, customField.getDataType()))
                .filter(Objects::nonNull)
                .toList();

        if (converted.isEmpty()) return null;

        if (customField.getIsArray()) {
            return converted;
        }

        return converted.getFirst();
    }

    private Object convertCustomFieldValue(String value, DataType dataType) {
        if (value == null) return null;
        if (dataType == null) return value;

        try {
            return switch (dataType) {
                case String -> value;
                case Boolean -> Boolean.parseBoolean(value);
                case Integer -> Integer.parseInt(value);
                case Long -> Long.parseLong(value);
                case Double -> Double.parseDouble(value);
            };
        } catch (Exception ex) {
            log.warn("Could not convert custom CKAN field value " + value+ " to type " + dataType.name());
            return value; // fallback: send as string
        }
    }

    private WebClient getWebClient(){
        return WebClient.builder().filters(exchangeFilterFunctions -> {
            exchangeFilterFunctions.add(logRequest());
            exchangeFilterFunctions.add(logResponse());
        }).codecs(codecs -> codecs
                .defaultCodecs()
                .maxInMemorySize(this.ckanServiceProperties.getMaxInMemorySizeInBytes())
        ).build();
    }

    private static ExchangeFilterFunction logRequest() {
        return ExchangeFilterFunction.ofRequestProcessor(clientRequest -> {
            logger.debug(new MapLogEntry("Request").And("method", clientRequest.method().toString()).And("url", clientRequest.url().toString()));
            return Mono.just(clientRequest);
        });
    }

    private static ExchangeFilterFunction logResponse() {
        return ExchangeFilterFunction.ofResponseProcessor(response -> {
            if (response.statusCode().isError()) {
                return response.mutate().build().bodyToMono(String.class)
                        .flatMap(body -> {
                            logger.error(new MapLogEntry("Response").And("method", response.request().getMethod().toString()).And("url", response.request().getURI()).And("status", response.statusCode().toString()).And("body", body));
                            return Mono.just(response);
                        });
            }
            return Mono.just(response);

        });
    }

}

