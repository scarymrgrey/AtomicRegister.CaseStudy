package com.example

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import com.example.Replica.{Ack, KVPair, ReadOPResponse, TMessage, WriteOP}

import scala.util.Random

object Client {

  val majQuo = 2

  final case class StartWriting()

  final case class StartReading()

  def apply(replicaSet: Set[ActorRef[TMessage]]): Behavior[Any] = {
    var reads: List[KVPair] = List()
    var counter = 0
    var acks = 0
    Behaviors.receive { (context, message) =>
      message match {
        case StartWriting() =>
          Random.shuffle(replicaSet).take(majQuo).foreach(_ ! Replica.WriteOPDelayed(KVPair("key1", counter, counter), context.self))

        case StartReading() =>
          Random.shuffle(replicaSet).take(majQuo).foreach(_ ! Replica.ReadOPDelayed("key1", context.self))

        case Ack() => synchronized {
          counter += 1
          acks += 1
          if (acks >= majQuo) {
            acks = 0
            context.self ! StartWriting()
          }
        }

        case ReadOPResponse(kv: Option[KVPair]) => synchronized {
          acks += 1
          kv match {
            case Some(el) =>  reads = el +: reads
            case _ =>
          }

          if (acks >= majQuo) {
            acks = 0
            val readval = reads match {
              case Nil => None
              case list => list.maxBy(_.version)
            }
            reads = List()
            context.log.info("Value read: {}", readval)
            context.self ! Client.StartReading()

          }
        }
      }
      Behaviors.same
    }
  }

  object NonEmpty {
    def unapply(l: List[_]): Option[List[_]] = l.headOption.map(_ => l)
  }
  def sequence[A](l: List[Option[A]]): Option[List[A]] = l.foldLeft(Option(List.empty[A])) {
    case (Some(acc), Some(value)) => Some(value :: acc);
    case (_, _) =>
      None
  }
}
