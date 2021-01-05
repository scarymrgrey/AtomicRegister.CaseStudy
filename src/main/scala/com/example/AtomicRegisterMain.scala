package com.example

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}

object Guardian {
  def apply(): Behavior[String] =
    Behaviors.setup { context =>
      val r1 = context.spawn(Replica(), "Replica-1")
      val r2 = context.spawn(Replica(), "Replica-2")
      val r3 = context.spawn(Replica(), "Replica-3")
      val writer = context.spawn(Client(Set(r1, r2, r3)), "writer")
      val reader = context.spawn(Client(Set(r1, r2, r3)), "reader")

      Behaviors.receiveMessage { message =>
        writer ! Client.StartWriting()
        reader ! Client.StartReading()
        Behaviors.same
      }
    }
}

object AtomicRegister extends App {
  val system: ActorSystem[String] = ActorSystem(Guardian(), "ABCaseStudy")
  system ! "start"
}

