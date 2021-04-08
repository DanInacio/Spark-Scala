/*
Scala Programming Assessment Test

Create Functions to solve the following questions!
The questions are named and then followed by a description.

1.) Check for Single Even:
Write a function that takes in an integer and returns a Boolean indicating whether
or not it is even. See if you can write this in one line!
*/
def checkEven(num:Int): Boolean = {
  if(num % 2 == 0){
    return true
  }else
  {
    return false
  }
}
checkEven(3)
checkEven(6)
print("\u001b[2J")

/*
2.) Check for Evens in a List:
Write a function that returns True if there is an even number inside of a List,
otherwise, return False.
*/
def checkEvenList(nums:List[Int]): Boolean = {
  for(num <- nums)
  {
    if(num % 2 == 0)
    {
      return true
    }
  }
  return false
}

val odds = List(1,3,5,7,9)
val oneEven = List(1,2,3)
checkEvenList(odds)
checkEvenList(oneEven)
print("\u001b[2J")

/*
3.) Lucky Number Seven:
Take in a list of integers and calculate their sum. However, sevens are lucky
and they should be counted twice, meaning their value is 14 for the sum. Assume
the list isn't empty.
*/
def luckyAdder(nums:List[Int]): Int = {
  var total = 0
  for(num <- nums)
  {
    if(num == 7)
    {
      total +=num*2
    }else
    {
      total += num
    }
  }
  return total
}

val numList = List(1,2,3,7)
luckyAdder(numList)
print("\u001b[2J")

/*
4.) Can you Balance?
Given a non-empty list of integers, return true if there is a place to
split the list so that the sum of the numbers on one side
is equal to the sum of the numbers on the other side. For example, given the
list (1,5,3,3) would return true, you can split it in the middle. Another
example (7,3,4) would return true 3+4=7. Remember you just need to return the
boolean, not the split index point.
*/
def splitter(nums:List[Int]): Boolean = {
  var sum1=0
  var sum2=0
  sum2 = nums.sum

  for(i <- Range(0,nums.length))
  {
    sum1 += nums(i)
    sum2 -= nums(i)

    if(sum1 == sum2)
    {
      return true
    }
  }
  return false
}

var splitList = List(1,2,3)
println(splitter(splitList))
print("\u001b[2J")

/*
5.) Palindrome Check
Given a String, return a boolean indicating whether or not it is a palindrome.
(Spelled the same forwards and backwards). Try exploring methods to help you.
*/
def palindrome(word:String): Boolean = {

  if(word == word.reverse)
  {
    return true
  }
  return false
}

println(palindrome("abba"))
println(palindrome("unknown"))
