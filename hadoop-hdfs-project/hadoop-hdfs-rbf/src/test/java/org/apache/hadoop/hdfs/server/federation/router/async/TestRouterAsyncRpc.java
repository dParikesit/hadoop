/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router.async;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedEntries;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.fairness.RouterAsyncRpcFairnessPolicyController;
import org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessPolicyController;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.TestRouterRpc;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_ENABLE_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.syncReturn;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Testing the asynchronous RPC functionality of the router.
 */
public class TestRouterAsyncRpc extends TestRouterRpc {
  private static MiniRouterDFSCluster cluster;
  private MiniRouterDFSCluster.RouterContext rndRouter;

  @BeforeAll
  public static void globalSetUp() throws Exception {
    // Start routers with only an RPC service.
    Configuration routerConf = new RouterConfigBuilder()
        .metrics()
        .rpc()
        .build();
    // We decrease the DN cache times to make the test faster.
    routerConf.setTimeDuration(
        RBFConfigKeys.DN_REPORT_CACHE_EXPIRE, 1, TimeUnit.SECONDS);
    // Use async router.
    routerConf.setBoolean(DFS_ROUTER_ASYNC_RPC_ENABLE_KEY, true);
    // Use RouterAsyncRpcFairnessPolicyController as the fairness controller.
    routerConf.setClass(DFS_ROUTER_FAIRNESS_POLICY_CONTROLLER_CLASS,
        RouterAsyncRpcFairnessPolicyController.class,
        RouterRpcFairnessPolicyController.class);
    routerConf.setInt(DFS_ROUTER_ASYNC_RPC_HANDLER_COUNT_KEY, 2);
    setUp(routerConf);
  }

  @BeforeEach
  public void testSetup() throws Exception {
    super.testSetup();
    cluster = super.getCluster();
    // Random router for this test.
    rndRouter = cluster.getRandomRouter();
  }

  @Test
  @Override
  public void testgetGroupsForUser() throws Exception {
    String[] group = new String[] {"bar", "group2"};
    UserGroupInformation.createUserForTesting("user",
        new String[] {"bar", "group2"});
    rndRouter.getRouter().getRpcServer().getGroupsForUser("user");
    String[] result = syncReturn(String[].class);
    assertArrayEquals(group, result);
  }

  @Test
  @Override
  public void testConcurrentCallExecutorInitial() {
    assertNull(rndRouter.getRouterRpcClient().getExecutorService());
  }

  @Test
  public void testGetDelegationTokenAsyncRpc() throws Exception {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    assertDoesNotThrow(() -> {
      rndRouter.getFileSystem().getDelegationToken(ugi.getShortUserName());
    });
  }

  @Test
  public void testProxyGetHAServiceStateAsync() throws Exception {
    HAServiceState state = getRouterProtocol().getHAServiceState();
    assertNotNull(state);
  }

  @Test
  public void testProxyListEncryptionZonesAsync() throws Exception {
    Method m = ClientProtocol.class.getMethod("listEncryptionZones", long.class);
    assertRouterAndNamenodeOutcome(m, new Object[] {0L}, new Object[] {0L});
  }

  @Test
  public void testProxyReencryptEncryptionZoneAsync() throws Exception {
    Method m = ClientProtocol.class.getMethod(
        "reencryptEncryptionZone", String.class, HdfsConstants.ReencryptAction.class);
    String zone = "/unknownlocation/unknowndir";
    assertRouterAndNamenodeOutcome(m,
        new Object[] {zone, HdfsConstants.ReencryptAction.START},
        new Object[] {zone, HdfsConstants.ReencryptAction.START});
  }

  @Test
  public void testProxyListReencryptionStatusAsync() throws Exception {
    Method m = ClientProtocol.class.getMethod("listReencryptionStatus", long.class);
    assertRouterAndNamenodeOutcome(m, new Object[] {0L}, new Object[] {0L});
  }

  @Test
  public void testProxyGetEditsFromTxidAsync() throws Exception {
    Method m = ClientProtocol.class.getMethod("getEditsFromTxid", long.class);
    long txid = getNamenodeProtocol().getCurrentEditLogTxid();
    assertRouterAndNamenodeOutcome(m, new Object[] {txid}, new Object[] {txid});
  }

  @Test
  public void testProxyGetDataEncryptionKeyAsync() throws Exception {
    Method m = ClientProtocol.class.getMethod("getDataEncryptionKey");
    assertRouterAndNamenodeOutcome(m, new Object[] {}, new Object[] {});
  }

  @Test
  public void testProxyReportBadBlocksAsync() throws Exception {
    Method m = ClientProtocol.class.getMethod("reportBadBlocks", LocatedBlock[].class);
    LocatedBlock[] noBlocks = new LocatedBlock[0];
    assertRouterAndNamenodeOutcome(
        m, new Object[] {noBlocks}, new Object[] {noBlocks});
  }

  @Test
  public void testProxyListOpenFilesAsync() throws Exception {
    String routerOpenFile = getRouterFile() + "-open";
    String nnOpenFile = getNamenodeFile() + "-open";
    String routerDir = routerOpenFile.substring(0, routerOpenFile.lastIndexOf('/'));
    String nnDir = nnOpenFile.substring(0, nnOpenFile.lastIndexOf('/'));
    Method m = ClientProtocol.class.getMethod(
        "listOpenFiles", long.class, EnumSet.class, String.class);
    EnumSet<OpenFilesIterator.OpenFilesType> openFilesTypes =
        EnumSet.of(OpenFilesIterator.OpenFilesType.ALL_OPEN_FILES);

    try (FSDataOutputStream out =
        getRouterFileSystem().create(new Path(routerOpenFile), true)) {
      out.write(1);
      assertRouterAndNamenodeOutcome(m,
          new Object[] {0L, openFilesTypes, routerDir},
          new Object[] {0L, openFilesTypes, nnDir});
    }
  }

  @Test
  public void testProxyListOpenFilesWithDefaultPathAsync() throws Exception {
    Method m = ClientProtocol.class.getMethod("listOpenFiles", long.class);
    assertRouterAndNamenodeOutcome(m, new Object[] {0L}, new Object[] {0L});
  }

  private void assertRouterAndNamenodeOutcome(
      Method method, Object[] routerParams, Object[] nnParams) {
    Object routerResult = null;
    Exception routerException = null;
    try {
      routerResult = method.invoke(getRouterProtocol(), routerParams);
    } catch (Exception ex) {
      routerException = ex;
    }

    Object nnResult = null;
    Exception nnException = null;
    try {
      nnResult = method.invoke(getNamenodeProtocol(), nnParams);
    } catch (Exception ex) {
      nnException = ex;
    }

    if (routerException == null && nnException == null) {
      assertEquals(nnResult == null, routerResult == null);
      if (routerResult instanceof BatchedEntries && nnResult instanceof BatchedEntries) {
        BatchedEntries<?> routerEntries = (BatchedEntries<?>) routerResult;
        BatchedEntries<?> nnEntries = (BatchedEntries<?>) nnResult;
        assertEquals(nnEntries.hasMore(), routerEntries.hasMore());
      }
      if (routerResult instanceof HAServiceState && nnResult instanceof HAServiceState) {
        assertEquals(nnResult, routerResult);
      }
      return;
    }

    assertNotNull(routerException);
    assertNotNull(nnException);
    assertEquals(getExceptionClass(routerException), getExceptionClass(nnException));
  }

  private static Class<?> getExceptionClass(Exception exception) {
    Throwable throwable = exception.getCause() == null
        ? exception : exception.getCause();
    if (throwable instanceof RemoteException) {
      Throwable unwrapped = ((RemoteException) throwable).unwrapRemoteException();
      return unwrapped.getClass();
    }
    return throwable.getClass();
  }
}
