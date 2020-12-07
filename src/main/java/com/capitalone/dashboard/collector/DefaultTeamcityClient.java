package com.capitalone.dashboard.collector;

import com.capitalone.dashboard.model.*;
import com.capitalone.dashboard.repository.CommitRepository;
import com.capitalone.dashboard.util.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestOperations;

import java.io.UnsupportedEncodingException;
import java.net.*;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * TeamcityClient implementation that uses RestTemplate and JSONSimple to
 * fetch information from Teamcity instances.
 */
@Component
public class DefaultTeamcityClient implements TeamcityClient {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultTeamcityClient.class);

    private final RestOperations rest;
    private final TeamcitySettings settings;

    private static final String PROJECT_API_URL_SUFFIX = "app/rest/projects";

    private static final String BUILD_DETAILS_URL_SUFFIX = "app/rest/builds";

    private static final String BUILD_TYPE_DETAILS_URL_SUFFIX = "app/rest/buildTypes";
    private CommitRepository commitRepository;

    @Autowired
    public DefaultTeamcityClient(Supplier<RestOperations> restOperationsSupplier, TeamcitySettings settings, CommitRepository commitRepository) {
        this.rest = restOperationsSupplier.get();
        this.settings = settings;
        this.commitRepository = commitRepository;
    }

    @Override
    public Map<TeamcityProject, Map<jobData, Set<BaseModel>>> getInstanceProjects(String instanceUrl) {
        LOG.debug("Enter getInstanceProjects");
        Map<TeamcityProject, Map<jobData, Set<BaseModel>>> result = new LinkedHashMap<>();
        for (String projectID : settings.getProjectIds()) {
            JSONArray buildTypes = new JSONArray();
            recursivelyFindBuildTypes(instanceUrl, projectID, buildTypes);
            constructProject(result, buildTypes, projectID, instanceUrl);
        }
        return result;
    }

    private void constructProject(Map<TeamcityProject, Map<jobData, Set<BaseModel>>> result, JSONArray buildTypes, String projectID, String instanceUrl) {
        for (Object buildType : buildTypes) {
            JSONObject jsonBuildType = (JSONObject) buildType;
            final String buildTypeID = getString(jsonBuildType, "id");
            try {
                if (isDeploymentBuildType(buildTypeID, instanceUrl)) continue;
                final String projectURL = getString(jsonBuildType, "webUrl");
                LOG.debug("Process projectName " + buildTypeID + " projectURL " + projectURL);
                getProjectDetails(projectID, buildTypeID, buildTypeID, projectURL, instanceUrl, result);
            } catch (URISyntaxException e) {
                LOG.error("wrong syntax url for loading jobs details", e);
            } catch (ParseException e) {
                LOG.error("Parsing jobs details on instance: " + instanceUrl, e);
            }
        }
    }


    private void recursivelyFindBuildTypes(String instanceUrl, String projectID, JSONArray buildTypes) {
        try {
            String url = joinURL(instanceUrl, new String[]{PROJECT_API_URL_SUFFIX + "/id:" + projectID});
            LOG.info("Fetching project details for {}", url);
            ResponseEntity<String> responseEntity = makeRestCall(url);
            if (responseEntity == null) {
                return;
            }
            String returnJSON = responseEntity.getBody();
            if (StringUtils.isEmpty(returnJSON)) {
                return;
            }
            JSONParser parser = new JSONParser();
            JSONObject object = (JSONObject) parser.parse(returnJSON);
            JSONObject subProjectsObject = (JSONObject) object.get("projects");
            JSONArray subProjects = getJsonArray(subProjectsObject, "project");
            JSONObject buildTypesObject = (JSONObject) object.get("buildTypes");
            JSONArray buildType = getJsonArray(buildTypesObject, "buildType");
            if (subProjects.size() == 0 && buildType.size() == 0) {
                return;
            }
            buildTypes.addAll(buildType);
            if (subProjects.size() > 0) {
                for (Object subProject : subProjects) {
                    JSONObject jsonSubProject = (JSONObject) subProject;
                    final String subProjectID = getString(jsonSubProject, "id");
                    recursivelyFindBuildTypes(instanceUrl, subProjectID, buildTypes);
                }
            }
        } catch (ParseException e) {
            LOG.error("Parsing jobs details on instance: " + instanceUrl, e);
        }
    }


    private Boolean isDeploymentBuildType(String buildTypeID, String instanceUrl) throws ParseException {
        try {
            String buildTypesUrl = joinURL(instanceUrl, new String[]{String.format("%s/id:%s", BUILD_TYPE_DETAILS_URL_SUFFIX, buildTypeID)});
            LOG.info("isDeploymentBuildType fetching build types details for {}", buildTypesUrl);
            ResponseEntity<String> responseEntity = makeRestCall(buildTypesUrl);
            String returnJSON = responseEntity.getBody();
            if (StringUtils.isEmpty(returnJSON)) {
                return false;
            }
            JSONParser parser = new JSONParser();
            JSONObject object = (JSONObject) parser.parse(returnJSON);

            if (object.isEmpty()) {
                return false;
            }
            JSONObject buildTypesObject = (JSONObject) object.get("settings");
            JSONArray properties = getJsonArray(buildTypesObject, "property");
            if (properties.size() == 0) {
                return false;
            }
            for (Object property : properties) {
                JSONObject jsonProperty = (JSONObject) property;
                String propertyName = jsonProperty.get("name").toString();
                if (!propertyName.equals("buildConfigurationType")) continue;
                String propertyValue = jsonProperty.get("value").toString();
                return propertyValue.equals("DEPLOYMENT");
            }
        } catch (HttpClientErrorException hce) {
            LOG.error("http client exception loading build details", hce);
        }
        return false;
    }


    @SuppressWarnings({"PMD.NPathComplexity", "PMD.ExcessiveMethodLength", "PMD.AvoidBranchingStatementAsLastInLoop", "PMD.EmptyIfStmt"})
    private void getProjectDetails(String projectID, String buildTypeID, String projectName, String projectURL, String instanceUrl,
                                   Map<TeamcityProject, Map<jobData, Set<BaseModel>>> result) throws URISyntaxException, ParseException {
        LOG.debug("getProjectDetails: projectName " + projectName + " projectURL: " + projectURL);

        Map<jobData, Set<BaseModel>> jobDataMap = new HashMap<>();

        TeamcityProject teamcityProject = new TeamcityProject();
        teamcityProject.setInstanceUrl(instanceUrl);
        teamcityProject.setJobName(projectName);
        teamcityProject.setJobUrl(projectURL);
        teamcityProject.getOptions().put("projectId", projectID);

        Set<BaseModel> builds = getBuildDetailsForTeamcityProject(buildTypeID, instanceUrl);

        jobDataMap.put(jobData.BUILD, builds);

        result.put(teamcityProject, jobDataMap);
    }


    private Set<BaseModel> getBuildDetailsForTeamcityProjectPaginated(String buildTypeID, String instanceUrl, int startCount, int buildsCount) throws ParseException {
        Set<BaseModel> builds = new LinkedHashSet<>();
        try {
            String allBuildsUrl = joinURL(instanceUrl, new String[]{BUILD_DETAILS_URL_SUFFIX});
            LOG.info("Fetching builds for project {}", allBuildsUrl);
            String url = joinURL(allBuildsUrl, new String[]{String.format("?locator=buildType:%s,count:%d,start:%d", buildTypeID, buildsCount, startCount)});
            ResponseEntity<String> responseEntity = makeRestCall(url);
            String returnJSON = responseEntity.getBody();
            if (StringUtils.isEmpty(returnJSON)) {
                return Collections.emptySet();
            }
            JSONParser parser = new JSONParser();
            JSONObject object = (JSONObject) parser.parse(returnJSON);

            if (object.isEmpty()) {
                return Collections.emptySet();
            }
            JSONArray jsonBuilds = getJsonArray(object, "build");
            for (Object build : jsonBuilds) {
                JSONObject jsonBuild = (JSONObject) build;
                // A basic Build object. This will be fleshed out later if this is a new Build.
                String buildID = jsonBuild.get("id").toString();
                LOG.debug(" buildNumber: " + buildID);
                Build teamcityBuild = new Build();
                teamcityBuild.setNumber(buildID);
                String buildURL = String.format("%s?locator=id:%s", allBuildsUrl, buildID); //String buildURL = getString(jsonBuild, "webUrl");
                LOG.debug(" Adding Build: " + buildURL);
                teamcityBuild.setBuildUrl(buildURL);
                teamcityBuild.setBuildStatus(getBuildStatus(jsonBuild));
                builds.add(teamcityBuild);
            }
        } catch (HttpClientErrorException hce) {
            LOG.error("http client exception loading build details", hce);
        }
        return builds;

    }

    private Set<BaseModel> getBuildDetailsForTeamcityProject(String buildTypeID, String instanceUrl) throws ParseException {
        Set<BaseModel> allBuilds = new LinkedHashSet<>();
        int startCount = 0;
        int buildsCount = 100;
        while (true) {
            Set<BaseModel> builds = getBuildDetailsForTeamcityProjectPaginated(buildTypeID, instanceUrl, startCount, buildsCount);
            if (builds.isEmpty()) {
                break;
            }
            allBuilds.addAll(builds);
            startCount += 100;
        }
        return allBuilds;
    }


    @Override
    public Build getBuildDetails(String buildUrl, String instanceUrl) {
        LOG.debug("getting build details");
        String formattedBuildUrl = formatBuildUrl(buildUrl);
        try {
            String url = rebuildJobUrl(formattedBuildUrl, instanceUrl);
            ResponseEntity<String> result = makeRestCall(url);
            String resultJSON = result.getBody();
            if (StringUtils.isEmpty(resultJSON)) {
                LOG.error("Error getting build details for. URL=" + url);
                return null;
            }
            JSONParser parser = new JSONParser();
            try {
                JSONObject buildJson = (JSONObject) parser.parse(resultJSON);
                String buildStatus = buildJson.get("state").toString();
                // Ignore jobs that are building
                if (buildStatus.equals("finished")) {
                    Build build = new Build();


                    long startTime = getTimeInMillis(buildJson.get("startDate").toString());
                    long endTime = getTimeInMillis(buildJson.get("finishDate").toString());
                    long duration = endTime - startTime;
                    build.setStartTime(startTime);
                    build.setEndTime(endTime);
                    build.setDuration(duration);

                    build.setNumber(buildJson.get("id").toString());
                    build.setBuildUrl(buildUrl);
                    build.setBuildUrl(formattedBuildUrl);
                    build.setTimestamp(System.currentTimeMillis());
                    build.setEndTime(build.getStartTime() + build.getDuration());
                    build.setBuildStatus(getBuildStatus(buildJson));


                    //For git SCM, add the repoBranches. For other SCM types, it's handled while adding changesets
                    build.getCodeRepos().addAll(getGitRepoBranch(buildJson));


                    // Need to handle duplicate changesets bug in Pipeline jobs
//                    Set<String> commitIds = new HashSet<>();
                    // This is empty for git
//                    Set<String> revisions = new HashSet<>();

                    JSONObject revisions = (JSONObject) buildJson.get("revisions");
                    if (revisions != null) {
                        addRevisions(build, revisions);
                    }
                    return build;
                }

            } catch (ParseException e) {
                LOG.error("Parsing build: " + formattedBuildUrl, e);
            }
        } catch (RestClientException rce) {
            LOG.error("Client exception loading build details: " + rce.getMessage() + ". URL =" + formattedBuildUrl);
        } catch (MalformedURLException mfe) {
            LOG.error("Malformed url for loading build details" + mfe.getMessage() + ". URL =" + formattedBuildUrl);
        } catch (URISyntaxException use) {
            LOG.error("Uri syntax exception for loading build details" + use.getMessage() + ". URL =" + formattedBuildUrl);
        } catch (RuntimeException re) {
            LOG.error("Unknown error in getting build details. URL=" + formattedBuildUrl, re);
        } catch (UnsupportedEncodingException unse) {
            LOG.error("Unsupported Encoding Exception in getting build details. URL=" + formattedBuildUrl, unse);
        }
        return null;
    }

    private void addRevisions(Build build, JSONObject revisions) {

        //((JSONObject)((JSONArray)((JSONObject)buildJson.get("revisions")).get("revision")).get(0)).get("version")
        //((JSONObject)((JSONArray)revisions.get("revision")).get(0)).get("version")
        //((JSONObject)(theRevisions.get("revision")).get(0)).get("version")


        Object revision = revisions.get("revision");
        if (revision == null) {
            LOG.warn("No revision detected for build " + build.getBuildUrl());
            return;
        }
        JSONArray theRevisions = (JSONArray) revision;
        if (theRevisions.size() < 1) {
            LOG.warn("No revision detected for build " + build.getBuildUrl());
            return;
        }
        if (theRevisions.size() > 1) {
            LOG.warn("Multiple revisions detected for build " + build.getBuildUrl() + ", considering the first");
        }
        String theCommitVersion = (String) ((JSONObject)theRevisions.get(0)).get("version");
        List<Commit> matchedCommits = commitRepository.findByScmRevisionNumber(theCommitVersion);
        if (matchedCommits.isEmpty()) {
            LOG.warn("Commit sha " + theCommitVersion + " not found in commit repository, skip adding to pipeline commits this time");
        } else {
            build.setSourceChangeSet(Collections.singletonList(matchedCommits.get(0)));
        }
    }

    private String formatBuildUrl(String buildUrl) {
        return buildUrl.split("\\?")[0] + "/" + buildUrl.split("=")[1];
    }

    private long getTimeInMillis(String startDate) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss");
        String dateWithoutOffset = startDate.substring(0, 15);
        String offset = startDate.substring(15);
        LocalDateTime formattedDateTime = LocalDateTime.parse(dateWithoutOffset, formatter);
        String formattedOffset = offset.substring(0, 3) + ":" + offset.substring(3);
        ZoneOffset zoneOffset = ZoneOffset.of(formattedOffset);
        return formattedDateTime.atOffset(zoneOffset).toEpochSecond() * 1000;
    }

    //This method will rebuild the API endpoint because the buildUrl obtained via Teamcity API
    //does not save the auth user info and we need to add it back.
    public static String rebuildJobUrl(String build, String server) throws URISyntaxException, MalformedURLException, UnsupportedEncodingException {
        URL instanceUrl = new URL(server);
        String userInfo = instanceUrl.getUserInfo();
        String instanceProtocol = instanceUrl.getProtocol();

        //decode to handle + in the job name.
        String buildEscapeChar = build.replace("+", "%2B");

        //decode to handle spaces in the job name.
        URL buildUrl = new URL(URLDecoder.decode(buildEscapeChar, "UTF-8"));
        String buildPath = buildUrl.getPath();

        String host = buildUrl.getHost();
        int port = buildUrl.getPort();
        URI newUri = new URI(instanceProtocol, userInfo, host, port, buildPath, null, null);
        return newUri.toString();
    }


    /**
     * Grabs changeset information for the given build.
     *
     * @param build     a Build
     * @param changeSet the build JSON object
     * @param commitIds the commitIds
     * @param revisions the revisions
     */
    private void addChangeSet(Build build, JSONObject changeSet, Set<String> commitIds, Set<String> revisions) {
        String scmType = getString(changeSet, "kind");
        Map<String, RepoBranch> revisionToUrl = new HashMap<>();

        // Build a map of revision to module (scm url). This is not always
        // provided by the Hudson API, but we can use it if available.
        // For git, this map is empty.
        for (Object revision : getJsonArray(changeSet, "revisions")) {
            JSONObject json = (JSONObject) revision;
            String revisionId = json.get("revision").toString();
            if (StringUtils.isNotEmpty(revisionId) && !revisions.contains(revisionId)) {
                RepoBranch rb = new RepoBranch();
                rb.setUrl(getString(json, "module"));
                rb.setType(RepoBranch.RepoType.fromString(scmType));
                revisionToUrl.put(revisionId, rb);
                build.getCodeRepos().add(rb);
            }
        }

        for (Object item : getJsonArray(changeSet, "items")) {
            JSONObject jsonItem = (JSONObject) item;
            String commitId = getRevision(jsonItem);
            if (StringUtils.isNotEmpty(commitId) && !commitIds.contains(commitId)) {
                SCM scm = new SCM();
                scm.setScmAuthor(getCommitAuthor(jsonItem));
                scm.setScmCommitLog(getString(jsonItem, "msg"));
                scm.setScmCommitTimestamp(getCommitTimestamp(jsonItem));
                scm.setScmRevisionNumber(commitId);
                RepoBranch repoBranch = revisionToUrl.get(scm.getScmRevisionNumber());
                if (repoBranch != null) {
                    scm.setScmUrl(repoBranch.getUrl());
                    scm.setScmBranch(repoBranch.getBranch());
                }

                scm.setNumberOfChanges(getJsonArray(jsonItem, "paths").size());
                build.getSourceChangeSet().add(scm);
                commitIds.add(commitId);
            }
        }
    }

    /**
     * Gathers repo urls, and the branch name from the last built revision.
     * Filters out the qualifiers from the branch name and sets the unqualified branch name.
     * We assume that all branches are in remotes/origin.
     */

    @SuppressWarnings("PMD")
    private List<RepoBranch> getGitRepoBranch(JSONObject buildJson) {
        List<RepoBranch> list = new ArrayList<>();

        JSONArray actions = getJsonArray(buildJson, "actions");
        for (Object action : actions) {
            JSONObject jsonAction = (JSONObject) action;
            if (jsonAction.size() > 0) {
                JSONObject lastBuiltRevision = null;
                JSONArray branches = null;
                JSONArray remoteUrls = getJsonArray((JSONObject) action, "remoteUrls");
                if (!remoteUrls.isEmpty()) {
                    lastBuiltRevision = (JSONObject) jsonAction.get("lastBuiltRevision");
                }
                if (lastBuiltRevision != null) {
                    branches = getJsonArray(lastBuiltRevision, "branch");
                }
                // As of git plugin 3.0.0, when multiple repos are configured in the git plugin itself instead of MultiSCM plugin,
                // they are stored unordered in a HashSet. So it's buggy and we cannot associate the correct branch information.
                // So for now, we loop through all the remoteUrls and associate the built branch(es) with all of them.
                if (branches != null && !branches.isEmpty()) {
                    for (Object url : remoteUrls) {
                        String sUrl = (String) url;
                        if (sUrl != null && !sUrl.isEmpty()) {
                            sUrl = removeGitExtensionFromUrl(sUrl);
                            for (Object branchObj : branches) {
                                String branchName = getString((JSONObject) branchObj, "name");
                                if (branchName != null) {
                                    String unqualifiedBranchName = getUnqualifiedBranch(branchName);
                                    RepoBranch grb = new RepoBranch(sUrl, unqualifiedBranchName, RepoBranch.RepoType.GIT);
                                    list.add(grb);
                                }
                            }
                        }
                    }
                }
            }
        }
        return list;
    }

    private String removeGitExtensionFromUrl(String url) {
        String sUrl = url;
        //remove .git from the urls
        if (sUrl.endsWith(".git")) {
            sUrl = sUrl.substring(0, sUrl.lastIndexOf(".git"));
        }
        return sUrl;
    }

    /**
     * Gets the unqualified branch name given the qualified one of the following forms:
     * 1. refs/remotes/<remote name>/<branch name>
     * 2. remotes/<remote name>/<branch name>
     * 3. origin/<branch name>
     * 4. <branch name>
     *
     * @param qualifiedBranch
     * @return the unqualified branch name
     */

    private String getUnqualifiedBranch(String qualifiedBranch) {
        String branchName = qualifiedBranch;
        Pattern pattern = Pattern.compile("(refs/)?remotes/[^/]+/(.*)|(origin[0-9]*/)?(.*)");
        Matcher matcher = pattern.matcher(branchName);
        if (matcher.matches()) {
            if (matcher.group(2) != null) {
                branchName = matcher.group(2);
            } else if (matcher.group(4) != null) {
                branchName = matcher.group(4);
            }
        }
        return branchName;
    }

    private long getCommitTimestamp(JSONObject jsonItem) {
        if (jsonItem.get("timestamp") != null) {
            return (Long) jsonItem.get("timestamp");
        } else if (jsonItem.get("date") != null) {
            String dateString = (String) jsonItem.get("date");
            try {
                return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse(dateString).getTime();
            } catch (java.text.ParseException e) {
                // Try an alternate date format...looks like this one is used by Git
                try {
                    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z").parse(dateString).getTime();
                } catch (java.text.ParseException e1) {
                    LOG.error("Invalid date string: " + dateString, e);
                }
            }
        }
        return 0;
    }

    private String getString(JSONObject json, String key) {
        return (String) json.get(key);
    }

    private String getRevision(JSONObject jsonItem) {
        // Use revision if provided, otherwise use id
        Long revision = (Long) jsonItem.get("revision");
        return revision == null ? getString(jsonItem, "id") : revision.toString();
    }

    private JSONArray getJsonArray(JSONObject json, String key) {
        Object array = json.get(key);
        return array == null ? new JSONArray() : (JSONArray) array;
    }

    private String getFullName(JSONObject author) {
        return getString(author, "fullName");
    }

    private String getCommitAuthor(JSONObject jsonItem) {
        // Use user if provided, otherwise use author.fullName
        JSONObject author = (JSONObject) jsonItem.get("author");
        return author == null ? getString(jsonItem, "user") : getFullName(author);
    }

    private BuildStatus getBuildStatus(JSONObject buildJson) {
        String status = buildJson.get("status").toString();
        switch (status) {
            case "SUCCESS":
                return BuildStatus.Success;
            case "UNSTABLE":
                return BuildStatus.Unstable;
            case "FAILURE":
                return BuildStatus.Failure;
            case "ABORTED":
                return BuildStatus.Aborted;
            default:
                return BuildStatus.Unknown;
        }
    }

    @SuppressWarnings("PMD")
    protected ResponseEntity<String> makeRestCall(String sUrl) {
        LOG.debug("Enter makeRestCall " + sUrl);
        List<String> apiKeys = settings.getApiKeys();
        if (apiKeys.isEmpty()) {
            return rest.exchange(sUrl, HttpMethod.GET, null, String.class);
        } else {
            //TODO apiKeys need not be an array
            return rest.exchange(sUrl, HttpMethod.GET, new HttpEntity<>(createAuthzHeader(apiKeys.get(0))), String.class);
        }
    }

    private static HttpHeaders createAuthzHeader(final String apiToken) {
        String authHeader = "Bearer " + apiToken;

        HttpHeaders headers = new HttpHeaders();
        headers.set(HttpHeaders.AUTHORIZATION, authHeader);
        return headers;
    }

    // join a base url to another path or paths - this will handle trailing or non-trailing /'s
    public static String joinURL(String base, String[] paths) {
        StringBuilder result = new StringBuilder(base);
        Arrays.stream(paths).map(path -> path.replaceFirst("^(/)+", "")).forEach(p -> {
            if (result.lastIndexOf("/") != result.length() - 1) {
                result.append('/');
            }
            result.append(p);
        });
        return result.toString();
    }
}
