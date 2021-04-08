for(num <- List(1,2,3))
{
  println("Hello")
  println(num)
}
print("\u001b[2J") // clear cmd!

for(num <- Array.range(0,5))
{
  println(num)
}
print("\u001b[2J") // clear cmd!

for(num <- Set(1,2,3)) // Sets have NO PARTICULAR ORDER!
{
  println(num)
}
print("\u001b[2J") // clear cmd!

for(num <- Range(0,10))
{
  if(num % 2 == 0)
  {
    println(s"$num is even")
  }else
  {
    println(s"$num is odd")
  }
}
print("\u001b[2J") // clear cmd!

val names = List("John", "Abe", "Cindy", "Catherine")
for(name <- names)
{
  if(name.startsWith("C"))
  {
    println(s"$name starts with a 'C'")
  }
}
