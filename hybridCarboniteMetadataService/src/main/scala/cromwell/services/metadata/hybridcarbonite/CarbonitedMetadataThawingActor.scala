package cromwell.services.metadata.hybridcarbonite

import akka.actor.{ActorRef, LoggingFSM, Props, Status}
import akka.pattern.pipe
import cats.data.NonEmptyList
import cromwell.core.io.{AsyncIo, DefaultIoCommandBuilder}
import cromwell.core.{RootWorkflowId, WorkflowId}
import cromwell.services.metadata.MetadataService._
import cromwell.services.metadata.hybridcarbonite.CarbonitedMetadataThawingActor._
import cromwell.util.JsonEditor
import io.circe.parser._
import io.circe.{Json, Printer}

import scala.concurrent.{ExecutionContext, Future}

final class CarbonitedMetadataThawingActor(carboniterConfig: HybridCarboniteConfig, serviceRegistry: ActorRef, ioActor: ActorRef) extends LoggingFSM[ThawingState, ThawingData] {

  implicit val ec: ExecutionContext = context.dispatcher

  val asyncIo = new AsyncIo(ioActor, DefaultIoCommandBuilder)

  startWith(PendingState, PendingData)

  when(PendingState) {
    case Event(action: WorkflowMetadataReadAction, PendingData) =>
      val gcsResponse = for {
        rawResponseFromIoActor <- asyncIo.contentAsStringAsync(carboniterConfig.makePath(action.workflowId), maxBytes = Option(30 * 1024 * 1024), failOnOverflow = true)
        parsedResponse <- Future.fromTry(parse(rawResponseFromIoActor).toTry.map(GcsMetadataResponse.apply))
      } yield parsedResponse

      gcsResponse pipeTo self

      val targetState = if (action.requiresLabels) {
        serviceRegistry ! GetRootAndSubworkflowLabels(action.workflowId)
        NeedsGcsAndLabelsState
      } else {
        NeedsOnlyGcsState
      }

      goto(targetState) using WorkingNoData(sender(), action)
  }

  when(NeedsOnlyGcsState) {
    case Event(GcsMetadataResponse(gcsResponse), WorkingNoData(requester, action)) =>
      requester ! ThawCarboniteSucceeded(gcsResponse.editFor(action).printWith(Printer.spaces2))
      stop()
  }

  when(NeedsGcsAndLabelsState) {
    // Successful Responses:
    case Event(GcsMetadataResponse(gcsResponse), WorkingSomeData(requester, action, _ , Some(labels))) =>
      requester ! ThawCarboniteSucceeded(merge(gcsResponse.editFor(action), labels))
      stop()

    case Event(RootAndSubworkflowLabelsLookupResponse(_, labels), WorkingSomeData(requester, action, Some(gcsResponse), _)) =>
      requester ! ThawCarboniteSucceeded(merge(gcsResponse.editFor(action), labels))
      stop()

    case Event(GcsMetadataResponse(gcsResponse), WorkingNoData(requester, action)) =>
      stay() using WorkingSomeData(requester, action, gcsResponse = Option(gcsResponse))

    case Event(RootAndSubworkflowLabelsLookupResponse(_, labels), WorkingNoData(requester, action)) =>
      stay() using WorkingSomeData(requester, action, labels = Option(labels))

    // Failures:
    case Event(RootAndSubworkflowLabelsLookupFailed(_, failure), data: WorkingData) =>
      data.requester ! ThawCarboniteFailed(failure)
      stop()
  }

  whenUnhandled {
    case Event(Status.Failure(failure), data: WorkingData) =>
      data.requester ! ThawCarboniteFailed(failure)
      stop()
    case other =>
      log.error(s"Programmer Error: Unexpected message to ${self.path.name}: $other")
      stay()
  }

  def merge(metadata: Json, labels: Map[WorkflowId, Map[String, String]]): String =
    JsonEditor.updateLabels(metadata, labels).printWith(Printer.spaces2)

}

object CarbonitedMetadataThawingActor {

  def props(carboniterConfig: HybridCarboniteConfig, serviceRegistry: ActorRef, ioActor: ActorRef): Props =
    Props(new CarbonitedMetadataThawingActor(carboniterConfig, serviceRegistry, ioActor))

  sealed trait ThawCarboniteMessage

  final case class ThawCarbonitedMetadata(workflowId: RootWorkflowId) extends ThawCarboniteMessage
  final case class ThawCarboniteSucceeded(value: String) extends ThawCarboniteMessage
  final case class ThawCarboniteFailed(reason: Throwable) extends ThawCarboniteMessage

  final case class GcsMetadataResponse(response: Json)

  sealed trait ThawingState
  case object PendingState extends ThawingState
  case object NeedsGcsAndLabelsState extends ThawingState
  case object NeedsOnlyGcsState extends ThawingState

  sealed trait ThawingData
  case object PendingData extends ThawingData
  sealed trait WorkingData extends ThawingData { def requester: ActorRef }
  final case class WorkingNoData(requester: ActorRef, action: WorkflowMetadataReadAction) extends WorkingData
  final case class WorkingSomeData(requester: ActorRef, action: WorkflowMetadataReadAction, gcsResponse: Option[Json] = None, labels: Option[Map[WorkflowId, Map[String, String]]] = None) extends WorkingData

  implicit class EnhancedJson(val json: Json) extends AnyVal {
    def editFor(action: WorkflowMetadataReadAction): Json = action match {
      case _: GetLogs =>
        JsonEditor.includeJson(json, NonEmptyList.of("stdout", "stderr", "backendLogs"))
      case _: WorkflowOutputs =>
        JsonEditor.includeJson(json, NonEmptyList.of("outputs"))
      case _: GetMetadataAction => ???
      case other =>
        throw new RuntimeException(s"""Programmer Error: Unexpected WorkflowMetadataReadAction message of type '${other.getClass.getSimpleName}': $other""")
    }
  }

  implicit class EnhancedWorkflowMetadataReadAction(val action: WorkflowMetadataReadAction) extends AnyVal {
    def requiresLabels: Boolean = action match {
      case _: GetLogs | _: WorkflowOutputs => false
      case q: GetMetadataAction if q.key.excludeKeysOption.exists { _.toList.contains("labels") } => false
      case q: GetMetadataAction if q.key.includeKeysOption.exists { _.toList.contains("labels") } => true
      case q: GetMetadataAction if q.key.includeKeysOption.isDefined => false
      case _ => true
    }
  }
}
