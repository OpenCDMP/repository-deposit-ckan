package org.opencdmp.deposit.ckan.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "semantics")
public class SemanticsProperties {
    private List<String> relationshipsAsObject;

    private List<String> relationshipsAsSubject;

    public List<String> getRelationshipsAsObject() {
        return relationshipsAsObject;
    }

    public void setRelationshipsAsObject(List<String> relationshipsAsObject) {
        this.relationshipsAsObject = relationshipsAsObject;
    }

    public List<String> getRelationshipsAsSubject() {
        return relationshipsAsSubject;
    }

    public void setRelationshipsAsSubject(List<String> relationshipsAsSubject) {
        this.relationshipsAsSubject = relationshipsAsSubject;
    }
}
