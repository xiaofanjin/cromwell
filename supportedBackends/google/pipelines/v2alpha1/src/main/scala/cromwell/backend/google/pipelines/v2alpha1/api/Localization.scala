package cromwell.backend.google.pipelines.v2alpha1.api

import com.google.api.services.genomics.v2alpha1.model.Mount
import cromwell.backend.google.pipelines.common.PipelinesApiConfigurationAttributes.LocalizationConfiguration
import cromwell.backend.google.pipelines.common.PipelinesApiJobPaths._
import cromwell.backend.google.pipelines.common.api.PipelinesApiRequestFactory.CreatePipelineParameters
import cromwell.backend.google.pipelines.v2alpha1.PipelinesConversions._
import cromwell.backend.google.pipelines.v2alpha1.ToParameter.ops._
import cromwell.backend.google.pipelines.v2alpha1.api.ActionBuilder.Labels.{Key, Value}
import cromwell.backend.google.pipelines.v2alpha1.api.ActionBuilder.cloudSdkShellAction
import cromwell.backend.google.pipelines.v2alpha1.api.ActionCommands.localizeFile


trait Localization {
  def localizeActions(createPipelineParameters: CreatePipelineParameters, mounts: List[Mount])(implicit localizationConfiguration: LocalizationConfiguration) = {
    val localizationLabel = Map(Key.Tag -> Value.Localization)

    val localizationContainerPath = createPipelineParameters.commandScriptContainerPath.sibling(LocalizationScriptName)
    val localizeLocalizationScript = cloudSdkShellAction(localizeFile(
      cloudPath = createPipelineParameters.cloudCallRoot / LocalizationScriptName,
      containerPath = localizationContainerPath))(mounts = mounts, labels = localizationLabel)

    val delocalizationContainerPath = createPipelineParameters.commandScriptContainerPath.sibling(DelocalizationScriptName)
    val localizeDelocalizationScript = cloudSdkShellAction(localizeFile(
      cloudPath = createPipelineParameters.cloudCallRoot / DelocalizationScriptName,
      containerPath = delocalizationContainerPath))(mounts = mounts, labels = localizationLabel)

    val runLocalizationScript = cloudSdkShellAction(
      s"/bin/bash $localizationContainerPath")(mounts = mounts, labels = localizationLabel)

    // Any "classic" PAPI v2 one-at-a-time localizations.
    val singletonLocalizations = createPipelineParameters.inputOutputParameters.fileInputParameters.flatMap(_.toActions(mounts).toList)

    val localizations = localizeLocalizationScript :: runLocalizationScript :: localizeDelocalizationScript :: singletonLocalizations

    ActionBuilder.annotateTimestampedActions("localization", Value.Localization)(localizations)
  }
}
