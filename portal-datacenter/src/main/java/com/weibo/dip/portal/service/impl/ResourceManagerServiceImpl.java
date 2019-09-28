package com.weibo.dip.portal.service.impl;

import com.weibo.dip.portal.conf.YarnMonitorSettings;
import com.weibo.dip.portal.dao.ResourceManagerDao;
import com.weibo.dip.portal.service.ResourceManagerService;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.util.*;

@Service("resourceManagerService")
@Transactional
public class ResourceManagerServiceImpl implements ResourceManagerService {
    private static final Logger LOGGER = LoggerFactory.getLogger("ResourceManagerServiceImpl");

    @Autowired
    private YarnMonitorSettings yarnMoniterSettings;

    @Autowired
    private ResourceManagerDao resourceManagerDao;
    // 全局变量，存放queueName
    private List<String> queueNameList = Collections.synchronizedList(new ArrayList<String>());

    @Override
    public List<String> getQueueName(String clusterId) {
        String schedulerInfoXml = null;
        Document document = null;

        String[] resourceManager = getResourceManager(clusterId);
        if (resourceManager.length != 2) {
            LOGGER.error("the number of resourcemanager should be 2, but the number is " + resourceManager.length);
            return null;
        }

        String schedulerUrl1 = "http://" + resourceManager[0] + "/ws/v1/cluster/scheduler";
        String schedulerUrl2 = "http://" + resourceManager[1] + "/ws/v1/cluster/scheduler";
        try {
            schedulerInfoXml = resourceManagerDao.getInfoXML(schedulerUrl1, schedulerUrl2);
            LOGGER.debug("schedulerInfoXml: " + schedulerInfoXml);
            document = DocumentHelper.parseText(schedulerInfoXml);
        } catch (Exception e) {
            LOGGER.error("get schedulerInfoXml or convert document error: " + ExceptionUtils.getFullStackTrace(e));
        }

        // 获取"rootQueue"节点
        Element node = document.getRootElement().element("schedulerInfo").element("rootQueue");

        // queueNameList是全局变量，必须先清空
        queueNameList.clear();
        if (node != null) {
            traverseNode(node);
        }

        LOGGER.debug("schedulerUrl1: " + schedulerUrl1);
        LOGGER.debug("schedulerUrl2: " + schedulerUrl2);
        LOGGER.debug("rootQueueNode: " + node.getName());
        for (String str : queueNameList) {
            LOGGER.info("queueName: " + str);
        }
        LOGGER.debug("queueNameList size: " + queueNameList.size());

        return queueNameList;
    }

    /**
     * 递归遍历node节点的所有子节点，将所有queueName存入queueNameList
     *
     * @param node
     */
    private void traverseNode(Element node) {
        // 取得node节点下所有名为"childQueues"的子节点
        @SuppressWarnings("unchecked")
        List<Element> nodes = node.elements("childQueues");

        for (Iterator<Element> it = nodes.iterator(); it.hasNext(); ) {
            Element ele = it.next();
            if (ele.attributeCount() != 0) {
                queueNameList.add(ele.element("queueName").getText());
            } else {
                traverseNode(ele);
            }
        }
    }

    @Override
    public Set<String> getUserName(String clusterId, String queueName) {
        Element rootNode = getRootNode(clusterId);
        if (rootNode == null) {
            return null;
        }

        Set<String> userNameSet = new TreeSet<String>();

        // 对根节点下的所有子节点进行遍历
        for (@SuppressWarnings("unchecked")
             Iterator<Element> it = rootNode.elementIterator(); it.hasNext(); ) {
            Element appNode = it.next();
            // 若app节点下的queue节点的值与queueName匹配,则获取user
            if (queueName.equals(appNode.element("queue").getText())) {
                String username = appNode.element("user").getText();
                userNameSet.add(username);
            }
        }
        LOGGER.debug("userNameSet size: " + userNameSet.size());

        return userNameSet;
    }

    @Override
    public Set<String> getApplicationType(String clusterId, String queueName, String userName) {
        Element rootNode = getRootNode(clusterId);
        if (rootNode == null) {
            return null;
        }

        Set<String> applicationTypeSet = new TreeSet<String>();

        // 对根节点下的所有子节点进行遍历
        for (@SuppressWarnings("unchecked")
             Iterator<Element> it = rootNode.elementIterator(); it.hasNext(); ) {
            Element appNode = it.next();
            // 若app节点下的queue节点的值与queueName且user节点的值与userName匹配,则获取applicationType
            if (queueName.equals(appNode.element("queue").getText()) && userName.equals(appNode.element("user").getText())) {
                String applicationType = appNode.element("applicationType").getText();
                applicationTypeSet.add(applicationType);
            }
        }
        LOGGER.debug("applicationTypeSet size: " + applicationTypeSet.size());

        return applicationTypeSet;
    }

    @Override
    public Map<String, List<String>> getApplication(String clusterId, String queueName, String userName, String applicationType) throws HttpException, IOException, DocumentException {
        Element rootNode = getRootNode(clusterId);
        if (rootNode == null) {
            return null;
        }

        Map<String, List<String>> applicationListMap = new TreeMap<String, List<String>>(
                new Comparator<String>() {
                    public int compare(String obj1, String obj2) {
                        //降序排列
                        return obj2.compareTo(obj1);
                    }
                });

        // 对根节点下的所有子节点进行遍历
        for (@SuppressWarnings("unchecked")
             Iterator<Element> it = rootNode.elementIterator(); it.hasNext(); ) {
            Element appNode = it.next();
            // 若app节点下的queue节点的值与queueName且user节点的值与userName匹配且applicationType节点与applicationType相同,则获取applicationName
            if (queueName.equals(appNode.element("queue").getText()) && userName.equals(appNode.element("user").getText()) && applicationType.equals(appNode.element("applicationType").getText())) {
                String applicationName = appNode.element("name").getText();
                String applicationId = appNode.element("id").getText();
                String startedTime = appNode.element("startedTime").getText();

                List<String> applicationList = new ArrayList<String>();
                applicationList.add(applicationId);
                applicationList.add(startedTime);

                if (applicationListMap.containsKey(applicationName)) {
                    // 若key相同则根据startedTime将最近的value覆盖旧value
                    List<String> applicationListOrig = applicationListMap.get(applicationName);
                    String startedTimeOrig = applicationListOrig.get(1);
                    if (Long.parseLong(startedTime) > Long.parseLong(startedTimeOrig)) {
                        applicationListMap.put(applicationName, applicationList);
                    }

                } else {
                    applicationListMap.put(applicationName, applicationList);
                }

            }
        }

        return applicationListMap;
    }


    /**
     * 获取root节点
     *
     * @param clusterId
     * @return Element
     */
    private Element getRootNode(String clusterId) {
        String[] resourceManager = getResourceManager(clusterId);
        if (resourceManager.length != 2) {
            LOGGER.error("the number of resourcemanager should be 2, but the number is " + resourceManager.length);
            return null;
        }
//		String address1 = "10.210.136.61:8088";
//		String address2 = "10.210.136.62:8088";
        // RUNNING，FINISHED
        String applicationsUrl1 = "http://" + resourceManager[0] + "/ws/v1/cluster/apps?state=RUNNING";
        String applicationsUrl2 = "http://" + resourceManager[1] + "/ws/v1/cluster/apps?state=RUNNING";

        String applicationInfoXml = null;
        Document document = null;

        // 获取http返回的xml信息
        try {
            applicationInfoXml = resourceManagerDao.getInfoXML(applicationsUrl1, applicationsUrl2);
            document = DocumentHelper.parseText(applicationInfoXml);
        } catch (Exception e) {
            LOGGER.error("get schedulerInfoXml or convert document error: " + ExceptionUtils.getFullStackTrace(e));
        }

        LOGGER.debug("applicationsUrl1: " + applicationsUrl1);
        LOGGER.debug("applicationsUrl2: " + applicationsUrl2);

        // 获取根节点
        Element rootNode = document.getRootElement();
        return rootNode;
    }

    @Override
    public String[] getResourceManager(String clusterId) {
        // clusterIdList的clusterId与resourceManagerList的resourceManager一一对应
        String[] clusterIdList = yarnMoniterSettings.getClusterIdList().split(",");
        String[] resourceManagerList = yarnMoniterSettings.getResourceManagerList().split("#");
        String[] resourceManager = null;

        for (int i = 0; i < clusterIdList.length; i++) {
            if (clusterIdList[i].equals(clusterId)) {
                resourceManager = resourceManagerList[i].split(",");
                break;
            }

        }

        return resourceManager;
    }


}
