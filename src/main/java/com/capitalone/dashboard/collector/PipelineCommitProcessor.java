package com.capitalone.dashboard.collector;

import com.capitalone.dashboard.model.*;
import com.capitalone.dashboard.repository.*;
import com.google.gson.Gson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.*;
import java.util.stream.Collectors;

@org.springframework.stereotype.Component
public class PipelineCommitProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineCommitProcessor.class);

    private final CollectorRepository collectorRepository;
    private final CollectorItemRepository collectorItemRepository;
    private final PipelineRepository pipelineRepository;
    private final ComponentRepository componentRepository;
    private final DashboardRepository dashboardRepository;

    @Autowired
    public PipelineCommitProcessor(CollectorRepository collectorRepository,
                                   @Qualifier("collectorItemRepository") CollectorItemRepository collectorItemRepository,
                                   PipelineRepository pipelineRepository,
                                   ComponentRepository componentRepository,
                                   DashboardRepository dashboardRepository) {
        this.collectorRepository = collectorRepository;
        this.collectorItemRepository = collectorItemRepository;
        this.pipelineRepository = pipelineRepository;
        this.componentRepository = componentRepository;
        this.dashboardRepository = dashboardRepository;
    }

    private List<Dashboard> findAllDashboardsForCollectorId(ObjectId collectorId, String gitProjectId) {
        List<CollectorItem> collectorItems = collectorItemRepository.findByCollectorIdIn(Collections.singletonList(collectorId));
        if (collectorItems == null || collectorItems.size() == 0) {
            return Collections.emptyList();
        }

        Optional<CollectorItem> collectorItemOptional =
                collectorItems.stream().filter(item ->
                        gitProjectId.equals(item.getOptions().get("projectId"))).findFirst();
        if (!collectorItemOptional.isPresent()) {
            return Collections.emptyList();
        }
        CollectorItem collectorItem = collectorItemOptional.get();
        List<Component> components = componentRepository
                .findByBuildCollectorItemId(collectorItem.getId());
        List<ObjectId> componentIds = components.stream().map(BaseModel::getId).collect(Collectors.toList());
        return dashboardRepository.findByApplicationComponentIdsIn(componentIds);
    }

    public void processPipelineCommits(List<PipelineCommit> commitsOfBuildStage, ObjectId collectorId, String gitProjectId) {
        if (commitsOfBuildStage.size() <= 0) {
            return;
        }
        List<Dashboard> allDashboardsForCommit = findAllDashboardsForCollectorId(collectorId, gitProjectId);
        List<String> dashBoardIds = allDashboardsForCommit.stream().map(d -> d.getId().toString()).collect(Collectors.toList());

        String buildEnvironment = PipelineStage.BUILD.getName();
        List<Collector> collectorList = collectorRepository.findByCollectorType(CollectorType.Product);
        List<CollectorItem> collectorItemList = collectorItemRepository.findByCollectorIdIn(collectorList.stream().map(BaseModel::getId).collect(Collectors.toList()));

        for (CollectorItem collectorItem : collectorItemList) {
            boolean dashboardId = dashBoardIds.contains(collectorItem.getOptions().get("dashboardId").toString());
            if (!dashboardId) {
                continue;
            }
            Pipeline pipeline = getOrCreatePipeline(collectorItem);
            Map<String, EnvironmentStage> environmentStageMap = pipeline.getEnvironmentStageMap();
            EnvironmentStage commitStage = environmentStageMap.get(PipelineStage.COMMIT.getName());
            if (commitStage == null || commitStage.getCommits() == null || commitStage.getCommits().isEmpty()) {
                LOG.error("Cannot populate pipeline commits for build since no pipeline commits for Commit stage found");
                LOG.error("Maybe the SCM collector has not been run?");
                continue;
            }
            List<PipelineCommit> pipelineCommitsOfCommitsStage = new ArrayList<>(commitStage.getCommits());
            pipelineCommitsOfCommitsStage.sort(Comparator.comparing(PipelineCommit::getScmCommitTimestamp).reversed());

            if (environmentStageMap.get(buildEnvironment) == null) {
                environmentStageMap.put(buildEnvironment, new EnvironmentStage());
            }

            EnvironmentStage environmentStage = environmentStageMap.get(buildEnvironment);
            if (environmentStage.getCommits() == null) {
                environmentStage.setCommits(new HashSet<>());
            }

            Set<PipelineCommit> buildStageCommits = new HashSet<>();
            //Add all existing commits and incoming commits, removing duplicates
            buildStageCommits.addAll(environmentStage.getCommits());
            buildStageCommits.addAll(commitsOfBuildStage);
            Map<String, PipelineCommit> builtCommitsBySha = buildStageCommits.stream()
                    .collect(Collectors.toMap(SCM::getScmRevisionNumber, x -> x));


            List<PipelineCommit> finalSetOfBuiltCommits = new ArrayList<>();
            long timestamp = 0;
            for (PipelineCommit commit :
                    pipelineCommitsOfCommitsStage) {
                if (builtCommitsBySha.containsKey(commit.getScmRevisionNumber())) {
                    PipelineCommit builtPipelineCommit = builtCommitsBySha.get(commit.getScmRevisionNumber());
                    finalSetOfBuiltCommits.add(builtPipelineCommit);
                    timestamp = builtPipelineCommit.getTimestamp();
                } else {
                    if (timestamp == 0) {
                        //Skip these commits because they may not have been built!
                        continue;
                    }
                    Gson gson = new Gson();
                    PipelineCommit pipelineCommit = gson.fromJson(gson.toJson(commit), PipelineCommit.class);
                    pipelineCommit.setTimestamp(timestamp);
                    finalSetOfBuiltCommits.add(pipelineCommit);
                }
            }
            LOG.info("Added {} pipeline commits to build stage", finalSetOfBuiltCommits.size());
            finalSetOfBuiltCommits.sort(Comparator.comparing(PipelineCommit::getTimestamp).reversed());
            environmentStage.setCommits(new LinkedHashSet<>(finalSetOfBuiltCommits));
            pipelineRepository.save(pipeline);
        }
    }
    protected Pipeline getOrCreatePipeline(CollectorItem collectorItem) {
        Pipeline pipeline = pipelineRepository.findByCollectorItemId(collectorItem.getId());
        if (pipeline == null) {
            pipeline = new Pipeline();
            pipeline.setCollectorItemId(collectorItem.getId());
        }
        return pipeline;
    }
}
