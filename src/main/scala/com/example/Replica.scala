package com.example

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import scala.concurrent.duration.DurationInt

object Replica {

  trait TMessage

  final case class KVPair(val key: String, value: Int, version: Int)

  final case class Ack() extends TMessage

  final case class ReadOP(key: String, sender: ActorRef[TMessage]) extends TMessage
  final case class ReadOPDelayed(key: String, sender: ActorRef[TMessage]) extends TMessage

  final case class WriteOP(k: KVPair, sender: ActorRef[TMessage]) extends TMessage
  final case class WriteOPDelayed(k: KVPair, sender: ActorRef[TMessage]) extends TMessage

  final case class ReadOPResponse(k: Option[KVPair]) extends TMessage


  def apply(): Behavior[TMessage] = {
    val dict: collection.mutable.Map[String, KVPair] = collection.mutable.Map()

    Behaviors.receive { (context, message) =>
      val r = scala.util.Random
      implicit val ec = context.system.executionContext
      message match {

        case  WriteOPDelayed(kv: KVPair, sender) =>
          context.system.scheduler.scheduleOnce((100 + r.nextInt(15000)) millis, () => context.self ! Replica.WriteOP(kv, sender))

        case  ReadOPDelayed(key: String, sender) =>
          context.system.scheduler.scheduleOnce((100 + r.nextInt(750)) millis, () => context.self ! Replica.ReadOP(key, sender))

        case WriteOP(kv: KVPair, sender) =>
          tryUpdateDict(dict, kv)
          sender ! Ack()

        case ReadOP(key,sender) =>
          val resp = tryGetValue(dict,key)
          sender ! ReadOPResponse(resp)
      }
      Behaviors.same
    }
  }

  private def tryUpdateDict(dict: collection.mutable.Map[String, KVPair], kv: KVPair): Unit = synchronized {
    dict.get(kv.key) match {
      case Some(oldKV) =>
        if (kv.version > oldKV.version)
          dict.update(kv.key, kv)

      case None => dict.update(kv.key, kv)
    }
  }

  private def tryGetValue(dict: collection.mutable.Map[String, KVPair], key: String): Option[KVPair] = synchronized {
    dict.get(key)
  }
}

