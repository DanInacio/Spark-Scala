def simple(): Unit = {
  println("simple print")
}

simple()
print("\u001b[2J")

///////////////////////////////////////////
def adder(num1:Int,num2:Int): Int = {
  return num1+num2
}

adder(4,5)
print("\u001b[2J")

///////////////////////////////////////////
def greetName(name:String): String = {
  return s"Hello $name"
}

val fullgreet = greetName("Daniel")
println(fullgreet)
print("\u001b[2J")

///////////////////////////////////////////
def isPrime(numCheck:Int): Boolean = {
  for(n <- Range(2,numCheck)){
    if(numCheck % n == 0)
    {
      return false
    }
  }
  return true
}

println(isPrime(10)) // False
println(isPrime(13)) // True
print("\u001b[2J")

///////////////////////////////////////////
val numbers = List(1,2,3,7)
def check(nums:List[Int]): List[Int] = {
  return nums
}
println(check(numbers))
