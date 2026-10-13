package org.opencdmp.deposit.ckan.configuration.preview;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Deployment-specific extra custom fields to expose in the deposit preview as
 * Manual fields (CKAN custom schema fields not covered by the built-in dataset
 * fields). CKAN exposes no generic API to introspect an instance's custom
 * schema, so the deployment declares here the keys its target instance
 * actually uses; a key the instance does not know is simply stored as an
 * extra/custom field, never an error.
 */
@ConfigurationProperties(prefix = "preview-extra-fields")
public class PreviewExtraFieldsProperties {

    private List<ExtraField> fields = new ArrayList<>();

    public List<ExtraField> getFields() {
        return fields;
    }

    public void setFields(List<ExtraField> fields) {
        this.fields = fields;
    }

    public static class ExtraField {

        /** The custom field key. Also used as the preview field code. */
        private String key;

        private String label;

        /** One of: text, textarea, tags, boolean, number, date. Defaults to text. */
        private String type;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }
}
