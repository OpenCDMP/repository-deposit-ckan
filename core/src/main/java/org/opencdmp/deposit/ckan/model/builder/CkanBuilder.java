package org.opencdmp.deposit.ckan.model.builder;

import gr.cite.tools.logging.LoggerService;
import gr.cite.tools.logging.MapLogEntry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opencdmp.commonmodels.enums.FieldType;
import org.opencdmp.commonmodels.enums.PlanAccessType;
import org.opencdmp.commonmodels.models.description.*;
import org.opencdmp.commonmodels.models.descriptiotemplate.DefinitionModel;
import org.opencdmp.commonmodels.models.descriptiotemplate.fielddata.RadioBoxDataModel;
import org.opencdmp.commonmodels.models.descriptiotemplate.fielddata.ReferenceTypeDataModel;
import org.opencdmp.commonmodels.models.descriptiotemplate.fielddata.SelectDataModel;
import org.opencdmp.commonmodels.models.plan.PlanBlueprintValueModel;
import org.opencdmp.commonmodels.models.plan.PlanContactModel;
import org.opencdmp.commonmodels.models.plan.PlanModel;
import org.opencdmp.commonmodels.models.planblueprint.SectionModel;
import org.opencdmp.commonmodels.models.planreference.PlanReferenceModel;
import org.opencdmp.commonmodels.models.reference.ReferenceModel;
import org.opencdmp.deposit.ckan.configuration.SemanticsProperties;
import org.opencdmp.deposit.ckan.model.CkanDataset;
import org.opencdmp.deposit.ckan.model.CkanExtra;
import org.opencdmp.deposit.ckan.model.CkanTag;
import org.opencdmp.deposit.ckan.model.DatasetRelationship;
import org.opencdmp.deposit.ckan.service.ckan.CkanDepositServiceImpl;
import org.opencdmp.deposit.ckan.service.ckan.CkanServiceProperties;
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

    private static final String SEMANTIC_DATASET_TYPE = "ckan.dataset.type";
    private static final String SEMANTIC_DATASET_ORGANIZATION = "ckan.dataset.organization";
    private static final String SEMANTIC_DATASET_TAGS = "ckan.dataset.tags";
    private static final String SEMANTIC_DATASET_EXTRAS = "ckan.dataset.extras";
    private static final String SEMANTIC_DATASET_LICENSE = "ckan.dataset.license";
    private static final String SEMANTIC_DATASET_SOURCE = "ckan.dataset.source";

    private final CkanServiceProperties ckanServiceProperties;
    private final SemanticsProperties semanticsProperties;

    @Autowired
    public CkanBuilder(CkanServiceProperties ckanServiceProperties, SemanticsProperties semanticsProperties){
            this.ckanServiceProperties = ckanServiceProperties;
        this.semanticsProperties = semanticsProperties;
    }

    public CkanDataset build(PlanModel planModel) {
        CkanDataset dataset = new CkanDataset();

        if (planModel == null) return dataset;

        dataset.setTitle(planModel.getLabel());
        dataset.setName(planModel.getId().toString());
        dataset.setPrivate(planModel.getAccessType().equals(PlanAccessType.Restricted));
        dataset.setNotes(planModel.getDescription());
        dataset.setVersion(String.valueOf(planModel.getVersion()));
        dataset.setType(this.applySingleValue(planModel, SEMANTIC_DATASET_TYPE));

        String url = this.applySingleValue(planModel, SEMANTIC_DATASET_SOURCE);
        if (url == null && planModel.getAccessType().equals(PlanAccessType.Public)) dataset.setUrl(this.ckanServiceProperties.getDomain() + "explore-plans/overview/public/" + planModel.getId().toString());
        else dataset.setUrl(url);

        this.applyOwnerOrg(planModel, dataset);
        this.applyTags(planModel, dataset);
        this.applyCreator(planModel, dataset);
        this.applyContacts(planModel, dataset);
        this.applyLicenses(planModel, dataset);
        this.applyExtras(planModel, dataset);

        return dataset;
    }

    public List<DatasetRelationship> buildDatasetRelationshipBySemantics(PlanModel planModel, String datasetId) {
        List<DatasetRelationship> relationships = new ArrayList<>();

        if (planModel == null || datasetId == null) return relationships;

        for (String semantic: semanticsProperties.getRelationshipsAsObject()) {
            this.buildDatasetRelationshipBySemantics(planModel, relationships, semantic, datasetId,true);
        }

        for (String semantic: semanticsProperties.getRelationshipsAsSubject()) {
            this.buildDatasetRelationshipBySemantics(planModel, relationships, semantic, datasetId,false);
        }

        return relationships;
    }

    private void buildDatasetRelationshipBySemantics(PlanModel planModel, List<DatasetRelationship> relationships, String semantic, String datasetId, boolean isRelationshipAsObject) {
        //plan blueprint semantics
        List<org.opencdmp.commonmodels.models.planblueprint.FieldModel> blueprintFieldsWithSemantic = this.getFieldOfSemantic(planModel, semantic);

        for (org.opencdmp.commonmodels.models.planblueprint.FieldModel blueprintFieldWithSemantic: blueprintFieldsWithSemantic) {
            PlanBlueprintValueModel planBlueprintValueModel = this.getPlanBlueprintValue(planModel, blueprintFieldWithSemantic.getId());
            if (planBlueprintValueModel != null && planBlueprintValueModel.getValue() != null && !planBlueprintValueModel.getValue().isBlank()) {
                DatasetRelationship relationship = new DatasetRelationship();
                relationship.setType(semantic.substring(semantic.lastIndexOf(".") + 1));
                if (isRelationshipAsObject) {
                    relationship.setObject(planBlueprintValueModel.getValue());
                    relationship.setSubject(datasetId);
                } else {
                    relationship.setObject(datasetId);
                    relationship.setSubject(planBlueprintValueModel.getValue());
                }

                if (!relationships.contains(relationship)) relationships.add(relationship);
            }
        }

        //description template semantics
        for (DescriptionModel descriptionModel: planModel.getDescriptions()) {
            List<org.opencdmp.commonmodels.models.descriptiotemplate.FieldModel> fieldsWithSemantics = this.findSchematicValues(semantic, descriptionModel.getDescriptionTemplate().getDefinition());
            Set<String> values = extractSchematicValues(fieldsWithSemantics, descriptionModel.getProperties());
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
    }

    private String applySingleValue(PlanModel planModel, String semanticCode) {

        // plan blueprint
        List<org.opencdmp.commonmodels.models.planblueprint.FieldModel> blueprintFieldsWithSemantics = this.getFieldOfSemantic(planModel, semanticCode);
        for (org.opencdmp.commonmodels.models.planblueprint.FieldModel field: blueprintFieldsWithSemantics) {
            PlanBlueprintValueModel planBlueprintValueModel = this.getPlanBlueprintValue(planModel, field.getId());
            if (planBlueprintValueModel != null) {
                if (planBlueprintValueModel.getValue() != null && !planBlueprintValueModel.getValue().isBlank()) return planBlueprintValueModel.getValue();
                if (planBlueprintValueModel.getDateValue() != null) return DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault()).format(planBlueprintValueModel.getDateValue());
                if (planBlueprintValueModel.getNumberValue() != null) return (planBlueprintValueModel.getNumberValue().toString());
            }

        }

        // description template
        if (planModel.getDescriptions() != null) {
            for (DescriptionModel descriptionModel: planModel.getDescriptions()) {
                List<org.opencdmp.commonmodels.models.descriptiotemplate.FieldModel> fieldsWithSemantics = this.findSchematicValues(semanticCode, descriptionModel.getDescriptionTemplate().getDefinition());
                Set<String> values = extractSchematicValues(fieldsWithSemantics, descriptionModel.getProperties());
                String value = values.stream().findFirst().orElse(null);
                if (value != null) {
                    return value;
                }
            }
        }

        return null;
    }

    private void applyCreator(PlanModel planModel, CkanDataset dataset) {
        if (planModel.getCreator() != null) {
            dataset.setAuthor(planModel.getCreator().getName());
            if (planModel.getCreator().getContacts() != null && !planModel.getCreator().getContacts().isEmpty()) dataset.setAuthorEmail(planModel.getCreator().getContacts().getFirst().getValue());
        }
    }

    private void applyContacts(PlanModel planModel, CkanDataset dataset) {
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
        }
    }

    private void applyLicenses(PlanModel planModel, CkanDataset dataset) {
        List<String> licenseIds = this.getLicenseIds();

        if (licenseIds != null && !licenseIds.isEmpty()) {
            String license = this.applySingleValue(planModel, SEMANTIC_DATASET_LICENSE);
            if (license == null) {
                List<ReferenceModel> planLicenses = this.getReferenceModelOfType(planModel, ckanServiceProperties.getLicenseReferenceCode());
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
        }
    }

    private void applyOwnerOrg(PlanModel planModel, CkanDataset dataset) {
        List<String> organizationIds = this.getOrganizationIds();

        if (organizationIds != null && !organizationIds.isEmpty()) {
            String organization = this.applySingleValue(planModel, SEMANTIC_DATASET_ORGANIZATION);
            if (organization == null) {
                List<ReferenceModel> planOrganizations = this.getReferenceModelOfType(planModel, ckanServiceProperties.getOrganizationReferenceCode());
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
        if (dataset.getOwnerOrg() == null) dataset.setOwnerOrg(this.ckanServiceProperties.getOrganization());
    }

    private void applyTags(PlanModel planModel, CkanDataset dataset) {
        List<CkanTag> tags = new ArrayList<>();

        //plan blueprint semantics
        List<org.opencdmp.commonmodels.models.planblueprint.FieldModel> blueprintFieldsWithSemantic = this.getFieldOfSemantic(planModel, SEMANTIC_DATASET_TAGS);
        for (org.opencdmp.commonmodels.models.planblueprint.FieldModel field: blueprintFieldsWithSemantic) {
            PlanBlueprintValueModel planBlueprintValueModel = this.getPlanBlueprintValue(planModel, field.getId());
            if (planBlueprintValueModel != null) {
                CkanTag tag = new CkanTag();
                if (planBlueprintValueModel.getValue() != null && !planBlueprintValueModel.getValue().isBlank()) tag.setName(planBlueprintValueModel.getValue());
                else if (planBlueprintValueModel.getNumberValue() != null) tag.setName(planBlueprintValueModel.getNumberValue().toString());
                else if (planBlueprintValueModel.getDateValue() != null) tag.setName(DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault()).format(planBlueprintValueModel.getDateValue()));

                if (tag.getName() != null && isValidTag(tag.getName()) && !tags.contains(tag)) tags.add(tag);
            }
        }

        //description template
        for (DescriptionModel descriptionModel: planModel.getDescriptions()) {
            List<org.opencdmp.commonmodels.models.descriptiotemplate.FieldModel> fieldsWithSemantics = this.findSchematicValues(SEMANTIC_DATASET_TAGS, descriptionModel.getDescriptionTemplate().getDefinition());
            Set<String> values = extractSchematicValues(fieldsWithSemantics, descriptionModel.getProperties());
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

        if (!tags.isEmpty()) dataset.setTags(tags);
    }

    private boolean isValidTag(String tag) {
        return Pattern.compile("^[a-zA-Z0-9 ._-]+$").matcher(tag).matches();
    }

    private void applyExtras(PlanModel planModel, CkanDataset dataset) {

        List<CkanExtra> extras = new ArrayList<>();

        //plan blueprint semantics
        List<org.opencdmp.commonmodels.models.planblueprint.FieldModel> blueprintFieldsWithSemantic = this.getFieldOfSemantic(planModel, SEMANTIC_DATASET_EXTRAS);
        for (org.opencdmp.commonmodels.models.planblueprint.FieldModel field: blueprintFieldsWithSemantic) {
            PlanBlueprintValueModel planBlueprintValueModel = this.getPlanBlueprintValue(planModel, field.getId());
            if (planBlueprintValueModel != null) {
                CkanExtra extra = new CkanExtra();
                extra.setKey(planBlueprintValueModel.getFieldId().toString());
                if (planBlueprintValueModel.getValue() != null && !planBlueprintValueModel.getValue().isBlank()) extra.setValue(planBlueprintValueModel.getValue());
                else if (planBlueprintValueModel.getNumberValue() != null) extra.setValue(planBlueprintValueModel.getNumberValue().toString());
                else if (planBlueprintValueModel.getDateValue() != null) extra.setValue(DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault()).format(planBlueprintValueModel.getDateValue()));
                if (extra.getValue() != null && !extras.contains(extra)) extras.add(extra);
            }
        }

        //description template semantics
        for (DescriptionModel descriptionModel: planModel.getDescriptions()) {
            List<org.opencdmp.commonmodels.models.descriptiotemplate.FieldModel> fieldsWithSemantics = this.findSchematicValues(SEMANTIC_DATASET_EXTRAS, descriptionModel.getDescriptionTemplate().getDefinition());
            for (org.opencdmp.commonmodels.models.descriptiotemplate.FieldModel field : fieldsWithSemantics) {
                if (field.getData() == null) continue;
                List<FieldModel> valueFields = this.findValueFieldsByIds(field.getId(),  descriptionModel.getProperties());
                for (FieldModel valueField : valueFields) {
                    CkanExtra extra = new CkanExtra();
                    extra.setKey(valueField.getId());
                    switch (field.getData().getFieldType()) {
                        case FREE_TEXT, TEXT_AREA, RICH_TEXT_AREA -> {
                            if (valueField.getTextValue() != null && !valueField.getTextValue().isBlank()) extra.setValue(valueField.getTextValue());
                        }
                        case BOOLEAN_DECISION, CHECK_BOX -> {
                            if (valueField.getBooleanValue() != null) extra.setValue(valueField.getBooleanValue().toString());
                        }
                        case DATE_PICKER -> {
                            if (valueField.getDateValue() != null) extra.setValue(DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault()).format(valueField.getDateValue()));
                        }
                        case DATASET_IDENTIFIER, VALIDATION -> {
                            if (valueField.getExternalIdentifier() != null && valueField.getExternalIdentifier().getIdentifier() != null && !valueField.getExternalIdentifier().getIdentifier().isBlank()) {
                                extra.setValue(valueField.getExternalIdentifier().getIdentifier());
                            }
                        }
                        case TAGS -> {
                            if (valueField.getTextListValue() != null && !valueField.getTextListValue().isEmpty()) {
                                extra.setValue(String.join(", ", valueField.getTextListValue()));
                            }
                        }
                        case SELECT -> {
                            if (valueField.getTextListValue() != null && !valueField.getTextListValue().isEmpty()) {
                                SelectDataModel selectDataModel = (SelectDataModel)field.getData();
                                if (selectDataModel != null && selectDataModel.getOptions() != null && !selectDataModel.getOptions().isEmpty()){
                                    List<String> values = new ArrayList<>();
                                    for (SelectDataModel.OptionModel option : selectDataModel.getOptions()){
                                        if (valueField.getTextListValue().contains(option.getValue()) || valueField.getTextListValue().contains(option.getLabel())) values.add(option.getLabel());
                                    }
                                    if (!values.isEmpty()) extra.setValue(String.join(", ", values));
                                }
                            }
                        }
                        case RADIO_BOX -> {
                            if (valueField.getTextListValue() != null && !valueField.getTextListValue().isEmpty()) {
                                RadioBoxDataModel radioBoxModel = (RadioBoxDataModel)field.getData();
                                if (radioBoxModel != null && radioBoxModel.getOptions() != null && !radioBoxModel.getOptions().isEmpty()){
                                    List<String> values = new ArrayList<>();
                                    for (RadioBoxDataModel.RadioBoxOptionModel option : radioBoxModel.getOptions()){
                                        if (valueField.getTextListValue().contains(option.getValue()) || valueField.getTextListValue().contains(option.getLabel())) values.add(option.getLabel());
                                    }
                                    if (!values.isEmpty()) extra.setValue(String.join(", ", values));
                                }
                            }
                        }
                        case REFERENCE_TYPES -> {
                            if (valueField.getReferences() != null && !valueField.getReferences().isEmpty()) {
                                List<String> referenceLabels = valueField.getReferences().stream().map(ReferenceModel::getLabel).toList();
                                if (!referenceLabels.isEmpty()) extra.setValue(String.join(", ", referenceLabels));
                            }
                        }
                    }
                    if (extra.getValue() != null && !extras.contains(extra)) extras.add(extra);
                }
            }
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


    private List<org.opencdmp.commonmodels.models.planblueprint.FieldModel> getFieldOfSemantic(PlanModel plan, String semanticKey){
        List<org.opencdmp.commonmodels.models.planblueprint.FieldModel> fields = new ArrayList<>();

        if (plan == null || plan.getPlanBlueprint() == null || plan.getPlanBlueprint().getDefinition() == null || plan.getPlanBlueprint().getDefinition().getSections() == null) return fields;
        for (SectionModel sectionModel : plan.getPlanBlueprint().getDefinition().getSections()){
            if (sectionModel.getFields() != null){
                org.opencdmp.commonmodels.models.planblueprint.FieldModel fieldModel = sectionModel.getFields().stream().filter(x-> x.getSemantics() != null && x.getSemantics().contains(semanticKey)).findFirst().orElse(null);
                if (fieldModel != null) fields.add(fieldModel);
            }
        }
        return fields;
    }

    private PlanBlueprintValueModel getPlanBlueprintValue(PlanModel plan, UUID id){
        if (plan == null || plan.getProperties() == null || plan.getProperties().getPlanBlueprintValues() == null) return null;
        return plan.getProperties().getPlanBlueprintValues().stream().filter(x-> x.getFieldId().equals(id)).findFirst().orElse(null);
    }

    private List<org.opencdmp.commonmodels.models.descriptiotemplate.FieldModel> findSchematicValues(String semantic, DefinitionModel definitionModel){
        return definitionModel.getAllField().stream().filter(x-> x.getSemantics() != null && x.getSemantics().contains(semantic)).toList();
    }

    private Set<String> extractSchematicValues(List<org.opencdmp.commonmodels.models.descriptiotemplate.FieldModel> fields, PropertyDefinitionModel propertyDefinition) {
        Set<String> values = new HashSet<>();
        for (org.opencdmp.commonmodels.models.descriptiotemplate.FieldModel field : fields) {
            if (field.getData() == null) continue;
            List<FieldModel> valueFields = this.findValueFieldsByIds(field.getId(), propertyDefinition);
            for (FieldModel valueField : valueFields) {
                switch (field.getData().getFieldType()) {
                    case FREE_TEXT, TEXT_AREA, RICH_TEXT_AREA -> {
                        if (valueField.getTextValue() != null && !valueField.getTextValue().isBlank()) values.add(valueField.getTextValue());
                    }
                    case BOOLEAN_DECISION, CHECK_BOX -> {
                        if (valueField.getBooleanValue() != null) values.add(valueField.getBooleanValue().toString());
                    }
                    case DATE_PICKER -> {
                        if (valueField.getDateValue() != null) values.add(DateTimeFormatter.ISO_DATE.format(valueField.getDateValue()));
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

    private List<FieldModel> findValueFieldsByIds(String fieldId, PropertyDefinitionModel definitionModel){
        List<FieldModel> models = new ArrayList<>();
        if (definitionModel == null || definitionModel.getFieldSets() == null || definitionModel.getFieldSets().isEmpty()) return models;
        for (PropertyDefinitionFieldSetModel propertyDefinitionFieldSetModel : definitionModel.getFieldSets().values()){
            if (propertyDefinitionFieldSetModel == null ||propertyDefinitionFieldSetModel.getItems() == null || propertyDefinitionFieldSetModel.getItems().isEmpty()) continue;
            for (PropertyDefinitionFieldSetItemModel propertyDefinitionFieldSetItemModel : propertyDefinitionFieldSetModel.getItems()){
                if (propertyDefinitionFieldSetItemModel == null ||propertyDefinitionFieldSetItemModel.getFields() == null || propertyDefinitionFieldSetItemModel.getFields().isEmpty()) continue;
                for (Map.Entry<String, FieldModel> entry : propertyDefinitionFieldSetItemModel.getFields().entrySet()){
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

