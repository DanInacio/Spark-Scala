import util.control.Breaks._ // Required to break from loops!

var x = 0
while(x < 5)
{
  println(s"x is currently $x")
  println("x is still less than 5, adding 1 to x")

  x += 1
}
print("\u001b[2J")

var y = 0
while(y < 10)
{
  println(s"y is currently $y")
  println("y is still less than 10, adding 1 to x")

  y += 1

  if(y==3)
  {
    break
  }
}
