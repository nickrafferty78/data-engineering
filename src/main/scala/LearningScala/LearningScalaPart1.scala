package LearningScala

object LearningScalaPart1 {


  def main(args: Array[String]): Unit = {
    //val is immutable - you should use this as much as possible
      val hello: String = "there"
    val singleLine: Float = 3.141598f

    println(helloThere(hello))
    println(otherValueTypes())
    //f formats and s evaluates variables
    println(f"Pi is about $singleLine%.3f")
    println(s"Testing variables $singleLine $hello")
    formatPi(singleLine)

    //controlFlow
    controlFlow()
    //fibonacci()
    //functions
    println(transformIt(3, squareIt))
    println(transformIt(2, x=> {val y=x*2; y*y}))

    //data structures
  tuples(captainStuff)
    lists(shiplist)
    println(backwardShips)
    println(sum)
  }

  def helloThere(word: String): String ={
    //make sure to add the return type correctly
    //you do not need return in Scala, it just returns the last value there
    val helloThere = "Hello " + word
   helloThere
  }

  def otherValueTypes():String={
    val numberOne : Int = 1
    val truth : Boolean = true
    val answer : String = "Answer"
    answer
  }

  def formatPi(pi: Float):Unit ={
    val doublePi = pi*2
    val ans = println(f"Value of double pi is $doublePi%.3f")
    ans
  }

  def controlFlow():Unit={
    if(1>3) println("impossible") else println("makes sense")
    //matching
    val number = 1
    number match {
      case 1 => println("one")
      case 2 => println("two")
    }

    //for loops
    for(x<-1 to 4){
      println(x)
    }
  }

  def fibonacci():Unit={
    var lower = 0
    println(lower)
    var higher = 1
    println(higher)
    for(x<-0 to 7){
      val current = lower + higher
      lower = higher
      higher = current
      println(current)
    }
  }
  //functions
  //every function in scala is almost it's own mini object
 def squareIt(x:Int): Int={
   x*x
 }
  //takes in a function that takes in an integer and returns an integer
  //return value of entire funtion is also an int
  def transformIt(x:Int, f:Int => Int): Int={
    f(x)
  }

  //lambda functions
  //this is a new function
  transformIt(3, x => x*x*x)
  transformIt(2, x=> {val y=x*2; y*y})


  //data structures
//Tuples - very common - immutable list
  val captainStuff = ("Pickard", "Nick", "Rafferty")

  //1 based index!!!!
  def tuples(x:(String,String,String)):Unit={
    println(x._1)
  }

  val shiplist = List("Enterprise", "Hello", "World")
  //lists
  //starts with 0
  def lists(x:List[String]):Unit={
    println(x(1))
  }

  val backwardShips = shiplist.map((ship: String)=> (ship.reverse))

    //reduce
  val numberList = List(1,2,3,4,5)
  val sum = numberList.reduce((x:Int, y:Int)=> x+y)


}
