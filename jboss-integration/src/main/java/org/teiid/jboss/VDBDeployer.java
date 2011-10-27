/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.jboss;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.Executor;

import javax.naming.InitialContext;

import org.jboss.as.server.deployment.Attachments;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.as.server.deployment.DeploymentUnit;
import org.jboss.as.server.deployment.DeploymentUnitProcessingException;
import org.jboss.as.server.deployment.DeploymentUnitProcessor;
import org.jboss.modules.Module;
import org.jboss.msc.service.*;
import org.jboss.msc.service.ServiceBuilder.DependencyType;
import org.jboss.msc.service.ServiceController.Mode;
import org.jboss.msc.service.ServiceController.State;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.deployers.TeiidAttachments;
import org.teiid.deployers.UDFMetaData;
import org.teiid.deployers.VDBRepository;
import org.teiid.deployers.VDBStatusChecker;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.jboss.VDBService.TranslatorNotFoundException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.index.IndexMetadataFactory;
import org.teiid.query.ObjectReplicator;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.services.BufferServiceImpl;
import org.teiid.translator.ExecutionFactory;


class VDBDeployer implements DeploymentUnitProcessor {
	private static final String JAVA_CONTEXT = "java:/"; //$NON-NLS-1$			
	private TranslatorRepository translatorRepository;
	private String asyncThreadPoolName;
	private VDBStatusChecker vdbStatusChecker;
	
	public VDBDeployer (TranslatorRepository translatorRepo, String poolName, VDBStatusChecker vdbStatusChecker) {
		this.translatorRepository = translatorRepo;
		this.asyncThreadPoolName = poolName;
		this.vdbStatusChecker = vdbStatusChecker;
	}
	
	public void deploy(final DeploymentPhaseContext context)  throws DeploymentUnitProcessingException {
		DeploymentUnit deploymentUnit = context.getDeploymentUnit();
		if (!TeiidAttachments.isVDBDeployment(deploymentUnit)) {
			return;
		}
		final String deploymentName = deploymentUnit.getName();
		final VDBMetaData deployment = deploymentUnit.getAttachment(TeiidAttachments.VDB_METADATA);
		
		// check to see if there is old vdb already deployed.
        final ServiceController<?> controller = context.getServiceRegistry().getService(TeiidServiceNames.vdbServiceName(deployment.getName(), deployment.getVersion()));
        if (controller != null) {
        	LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("redeploying_vdb", deployment)); //$NON-NLS-1$
            controller.setMode(ServiceController.Mode.REMOVE);
        }
		
		boolean preview = deployment.isPreview();
		if (!preview) {
			List<String> errors = deployment.getValidityErrors();
			if (errors != null && !errors.isEmpty()) {
				throw new DeploymentUnitProcessingException(RuntimePlugin.Util.getString("validity_errors_in_vdb", deployment)); //$NON-NLS-1$
			}
		}
				
		// add required connector managers; if they are not already there
		for (Translator t: deployment.getOverrideTranslators()) {
			VDBTranslatorMetaData data = (VDBTranslatorMetaData)t;
			
			String type = data.getType();
			Translator parent = this.translatorRepository.getTranslatorMetaData(type);
			if ( parent == null) {
				throw new DeploymentUnitProcessingException(RuntimePlugin.Util.getString("translator_type_not_found", deploymentName)); //$NON-NLS-1$
			}
		}
		
		// check if this is a VDB with index files, if there are then build the TransformationMetadata
		UDFMetaData udf = deploymentUnit.getAttachment(TeiidAttachments.UDF_METADATA);
		if (udf != null) {
			final Module module = deploymentUnit.getAttachment(Attachments.MODULE);
			if (module != null) {
				udf.setFunctionClassLoader(module.getClassLoader());
			}
			deployment.addAttchment(UDFMetaData.class, udf);
		}

		IndexMetadataFactory indexFactory = deploymentUnit.getAttachment(TeiidAttachments.INDEX_METADATA);
		if (indexFactory != null) {
			deployment.addAttchment(IndexMetadataFactory.class, indexFactory);
		}

		// remove the metadata objects as attachments
		deploymentUnit.removeAttachment(TeiidAttachments.INDEX_METADATA);
		deploymentUnit.removeAttachment(TeiidAttachments.UDF_METADATA);
		
		// build a VDB service
		ArrayList<String> unAvailableDS = new ArrayList<String>();
		VDBService vdb = new VDBService(deployment);
		final ServiceBuilder<VDBMetaData> vdbService = context.getServiceTarget().addService(TeiidServiceNames.vdbServiceName(deployment.getName(), deployment.getVersion()), vdb);
		for (ModelMetaData model:deployment.getModelMetaDatas().values()) {
			for (String sourceName:model.getSourceNames()) {
				if (!isSourceAvailable(model.getSourceConnectionJndiName(sourceName))) { 
					unAvailableDS.add(model.getSourceConnectionJndiName(sourceName));
				}
			}
		}
		
		// add dependencies to data-sources
		dataSourceDependencies(deployment, new DependentServices() {
			@Override
			public void serviceFound(String dsName, ServiceName svcName) {
				DataSourceListener dsl = new DataSourceListener(dsName, svcName, vdbStatusChecker);									
				ServiceBuilder<DataSourceListener> sb = context.getServiceTarget().addService(TeiidServiceNames.dsListenerServiceName(dsName), dsl);
				sb.addDependency(svcName);
				sb.setInitialMode(Mode.PASSIVE).install();
			}
		});
		
		// adding the translator services is redundant, however if one is removed then it is an issue.
		for (Model model:deployment.getModels()) {
			List<String> sourceNames = model.getSourceNames();
			for (String sourceName:sourceNames) {
				String translatorName = model.getSourceTranslatorName(sourceName);
				if (!deployment.isOverideTranslator(translatorName)) {
					vdbService.addDependency(TeiidServiceNames.translatorServiceName(translatorName));
				}
			}
		}
		
		//override translators (if any)
		for (Translator t: deployment.getOverrideTranslators()) {
			VDBTranslatorMetaData data = (VDBTranslatorMetaData)t;
			String type = data.getType();
			vdbService.addDependency(TeiidServiceNames.translatorServiceName(type));
		}	
		
		vdbService.addDependency(TeiidServiceNames.VDB_REPO, VDBRepository.class,  vdb.getVDBRepositoryInjector());
		vdbService.addDependency(TeiidServiceNames.TRANSLATOR_REPO, TranslatorRepository.class,  vdb.getTranslatorRepositoryInjector());
		vdbService.addDependency(TeiidServiceNames.executorServiceName(this.asyncThreadPoolName), Executor.class,  vdb.getExecutorInjector());
		vdbService.addDependency(TeiidServiceNames.OBJECT_SERIALIZER, ObjectSerializer.class, vdb.getSerializerInjector());
		vdbService.addDependency(TeiidServiceNames.BUFFER_MGR, BufferServiceImpl.class, vdb.getBufferServiceInjector());
		vdbService.addDependency(DependencyType.OPTIONAL, TeiidServiceNames.OBJECT_REPLICATOR, ObjectReplicator.class, vdb.getObjectReplicatorInjector());
		vdbService.setInitialMode(Mode.PASSIVE).install();
		
		if (!unAvailableDS.isEmpty()) {
			LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("vdb-inactive", deployment.getName(), deployment.getVersion(), unAvailableDS)); //$NON-NLS-1$
		}
	}
	
	private void dataSourceDependencies(VDBMetaData deployment, DependentServices svcListener) throws DeploymentUnitProcessingException {
		
		for (ModelMetaData model:deployment.getModelMetaDatas().values()) {
			for (String sourceName:model.getSourceNames()) {
				String translatorName = model.getSourceTranslatorName(sourceName);
				if (deployment.isOverideTranslator(translatorName)) {
					VDBTranslatorMetaData translator = deployment.getTranslator(translatorName);
					translatorName = translator.getType();
				}
				
				boolean jdbcSource = true;
				try {
					ExecutionFactory ef = VDBService.getExecutionFactory(translatorName, new TranslatorRepository(), this.translatorRepository, deployment, new IdentityHashMap<Translator, ExecutionFactory<Object, Object>>(), new HashSet<String>());
					jdbcSource = ef.isJDBCSource();
				} catch (TranslatorNotFoundException e) {
					if (e.getCause() != null) {
						throw new DeploymentUnitProcessingException(e.getCause());
					}
					throw new DeploymentUnitProcessingException(e.getMessage());						
				}

				// Need to make the data source service as dependency; otherwise dynamic vdbs will not work correctly.
				String dsName = model.getSourceConnectionJndiName(sourceName);
				ServiceName svcName = ServiceName.JBOSS.append("data-source", getJndiName(dsName)); //$NON-NLS-1$
				if (!jdbcSource) {
					svcName = ServiceName.JBOSS.append("connector", "connection-factory", getJndiName(dsName)); //$NON-NLS-1$ //$NON-NLS-2$
				}
				svcListener.serviceFound(dsName, svcName);				
			}
		}
	}
	
	interface DependentServices {
		void serviceFound(String dsName, ServiceName svc);
	}
	
	static class DataSourceListener implements Service<DataSourceListener>{
		private VDBStatusChecker vdbStatusChecker;
		private String dsName;
		private ServiceName svcName;
		
		public DataSourceListener(String dsName, ServiceName svcName, VDBStatusChecker checker) {
			this.dsName = dsName;
			this.svcName = svcName;
			this.vdbStatusChecker = checker;
		}
		
		public DataSourceListener getValue() throws IllegalStateException,IllegalArgumentException {
			return this;
		}

		@Override
		public void start(StartContext context) throws StartException {
			ServiceController s = context.getController().getServiceContainer().getService(this.svcName);
			if (s != null) {
				this.vdbStatusChecker.dataSourceAdded(this.dsName);
			}
		}

		@Override
		public void stop(StopContext context) {
			ServiceController s = context.getController().getServiceContainer().getService(this.svcName);
			if (s.getMode().equals(Mode.REMOVE) || s.getState().equals(State.STOPPING)) {
				this.vdbStatusChecker.dataSourceRemoved(this.dsName);
			}
		}		
	}

	private boolean isSourceAvailable(String name) {
    	String jndiName = getJndiName(name);
		try {
			InitialContext ic = new InitialContext();    		
			try {
				ic.lookup(jndiName);
			} catch (Exception e) {
				if (!jndiName.equals(name)) {
					ic.lookup(name);
				}
			}
		} catch (Exception e) {
			return false;
		}   			
    	return true;
	}

	private String getJndiName(String name) {
		String jndiName = name;
		if (!name.startsWith(JAVA_CONTEXT)) {
			jndiName = JAVA_CONTEXT + jndiName;
		}
		return jndiName;
	}	
	
	@Override
	public void undeploy(final DeploymentUnit deploymentUnit) {
		if (!TeiidAttachments.isVDBDeployment(deploymentUnit)) {
			return;
		}		
		
		final VDBMetaData deployment = deploymentUnit.getAttachment(TeiidAttachments.VDB_METADATA);
        final ServiceController<?> controller = deploymentUnit.getServiceRegistry().getService(TeiidServiceNames.vdbServiceName(deployment.getName(), deployment.getVersion()));
        if (controller != null) {
        	VDBService vdbService = (VDBService)controller.getService();
        	vdbService.undeployInProgress();
        	
    		try {
				dataSourceDependencies(deployment, new DependentServices() {
					@Override
					public void serviceFound(String dsName, ServiceName svcName) {
						ServiceController<?> controller = deploymentUnit.getServiceRegistry().getService(TeiidServiceNames.dsListenerServiceName(dsName));
						if (controller != null) {
							controller.setMode(ServiceController.Mode.REMOVE);
						}
					}
				});
			} catch (DeploymentUnitProcessingException e) {
				LogManager.logDetail(LogConstants.CTX_RUNTIME, e, IntegrationPlugin.Util.getString("vdb-undeploy-failed", deployment.getName(), deployment.getVersion())); //$NON-NLS-1$
			}        	
            controller.setMode(ServiceController.Mode.REMOVE);
        }
	}

}
