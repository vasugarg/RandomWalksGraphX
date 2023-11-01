package NetGraphAlgebraDefs

trait NetGraphComponent extends Serializable

@SerialVersionUID(123L)
case class NodeObject(
                       id: Int,
                       children: Int,
                       props: Int,
                       currentDepth: Int = 1,
                       propValueRange: Int,
                       maxDepth: Int,
                       maxBranchingFactor: Int,
                       maxProperties: Int,
                       storedValue: Double,
                       valuableData: Boolean = false
                     ) extends NetGraphComponent

@SerialVersionUID(123L)
case class Action(
                   actionType: Int,
                   fromNode: NodeObject,
                   toNode: NodeObject,
                   fromId: Int,
                   toId: Int,
                   resultingValue: Option[Int],
                   cost: Double
                 ) extends NetGraphComponent

@SerialVersionUID(123L)
case object TerminalNode extends NetGraphComponent

@SerialVersionUID(123L)
case object TerminalAction extends NetGraphComponent

