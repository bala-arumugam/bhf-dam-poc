package com.bhf.dam.poc.core.workflows;

import com.day.cq.dam.api.Asset;
import com.day.cq.dam.api.DamConstants;
import com.day.cq.dam.api.Rendition;
import com.day.cq.search.*;
import com.day.cq.search.result.Hit;
import com.day.cq.workflow.WorkflowSession;
import com.day.cq.workflow.exec.WorkItem;
import com.day.cq.workflow.exec.WorkflowProcess;
import com.day.cq.workflow.metadata.MetaDataMap;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.*;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Component(service = WorkflowProcess.class, property = {"process.label=Detect Duplicate DAM Assets (Checksum)"})
public class DuplicateAssetDetectionProcess implements WorkflowProcess {

    private static final Logger LOG = LoggerFactory.getLogger(DuplicateAssetDetectionProcess.class);

    private static final String SUBSERVICE_NAME = "dam-service";
    private static final String METADATA_PATH = "jcr:content/metadata";
    private static final String CHECKSUM_PROP = "dam:checksum";
    private static final String DUPLICATE_PROP = "dam:isDuplicate";
    private static final String PRIMARY_ASSET_PATH = "dam:primaryAssetPath";

    @Reference
    private ResourceResolverFactory resolverFactory;

    @Reference
    private QueryBuilder queryBuilder;

    @Override
    public void execute(WorkItem workItem, WorkflowSession workflowSession, MetaDataMap metaDataMap) {

        String payload = workItem.getWorkflowData().getPayload().toString();

        Map<String, Object> authInfo = new HashMap<>();
        authInfo.put(ResourceResolverFactory.SUBSERVICE, SUBSERVICE_NAME);

        try (ResourceResolver resolver = resolverFactory.getServiceResourceResolver(authInfo)) {

            // Case 1: Multiple assets
            if (payload.contains(",")) {
                for (String path : payload.split(",")) {
                    String trimmedPath = path.trim();
                    if (!processPath(trimmedPath, resolver)) {
                        LOG.warn("Asset path does not exist: {}", trimmedPath);
                    }
                }
            } else {
                // Case 2 & 3: Single asset or folder
                Resource payloadResource = resolver.getResource(payload);
                if (payloadResource == null) {
                    LOG.warn("Payload resource does not exist: {}", payload);
                    return;
                }

                // Case 2: Single asset
                if (payloadResource.isResourceType(DamConstants.NT_DAM_ASSET)) {
                    processAsset(payloadResource, resolver);
                }
                // Case 3: Folder
                else {
                    processFolder(payloadResource, resolver);
                }
            }

            resolver.commit();

        } catch (Exception e) {
            LOG.error("Error during duplicate asset detection: {}", e.getMessage());
        }
    }

    private void processFolder(Resource folder, ResourceResolver resolver) throws Exception {

        for (Resource child : folder.getChildren()) {

            // Asset
            if (child.isResourceType(DamConstants.NT_DAM_ASSET)) {
                processAsset(child, resolver);
            }
            // Nested folder
            else {
                processFolder(child, resolver);
            }
        }
    }

    private void processAsset(Resource assetResource, ResourceResolver resolver) throws Exception {

        Asset asset = assetResource.adaptTo(Asset.class);
        if (asset == null) return;

        Rendition original = asset.getRendition("original");
        if (original == null) return;

        Resource metadataRes = assetResource.getChild(METADATA_PATH);
        if (metadataRes == null) return;

        ModifiableValueMap metadata = metadataRes.adaptTo(ModifiableValueMap.class);

        try (InputStream is = original.getStream()) {
            String checksum = DigestUtils.sha256Hex(is);

            // Skip if already processed
            if (checksum.equals(metadata.get(CHECKSUM_PROP, String.class))) {
                return;
            }

            metadata.put(CHECKSUM_PROP, checksum);

            //  duplicate lookup
            String primaryAssetPath = findDuplicate(assetResource.getPath(), checksum, resolver);

            if (!Objects.equals(primaryAssetPath, StringUtils.EMPTY)) {
                metadata.put(DUPLICATE_PROP, true);
                metadata.put(PRIMARY_ASSET_PATH, primaryAssetPath);
            } else {
                metadata.put(DUPLICATE_PROP, false);
                metadata.remove(PRIMARY_ASSET_PATH);
            }
        }
    }

    private boolean processPath(String path, ResourceResolver resolver) throws Exception {
        Resource resource = resolver.getResource(path);
        if (resource == null) return false;

        // Asset
        if (resource.isResourceType(DamConstants.NT_DAM_ASSET)) {
            processAsset(resource, resolver);
        }
        // Nested folder
        else {
            processFolder(resource, resolver);
        }
        return true;
    }

    private String findDuplicate(String assetPath, String checksum, ResourceResolver resolver) throws RepositoryException {

        Session session = resolver.adaptTo(Session.class);

        Map<String, String> predicates = new HashMap<>();
        predicates.put("type", DamConstants.NT_DAM_ASSET);
        predicates.put("path", "/content/dam");
        predicates.put("property", METADATA_PATH + "/" + CHECKSUM_PROP);
        predicates.put("property.value", checksum);
        predicates.put("p.limit", "-1");

        Query query = queryBuilder.createQuery(PredicateGroup.create(predicates), session);

        for (Hit hit : query.getResult().getHits()) {
            if (!hit.getPath().equals(assetPath)) {
                return hit.getPath();
            }
        }
        return StringUtils.EMPTY;
    }
}
