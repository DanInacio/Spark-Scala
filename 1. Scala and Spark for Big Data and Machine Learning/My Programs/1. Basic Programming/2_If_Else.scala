//Control Flow
// if, else if, and else

// Use these to control the flow of your code based on booleans
// Some Java developers like to put semicolons, but Scala developers
// typically omit them.

////////////////////////
//// OVERALL FORMAT ////
////////////////////////
//   if(boolean){
//     do something
//   }else if(boolean){
//     do something else
//   }else if(boolean){
//     do something else
//   }else{
//     do something if none of the booleans were true
//   }
//

if(true)
{
  println("True")
}

val x = "Hell"
if(x.endsWith("o"))
{
  println("Value of x ends with 'o'")
}else
{
  println("Value of x does not end with o")
}

val person = "Marth"
if(person=="Sammy")
{
  println("Welcome, Sammy")
}else if(person=="George")
{
  println("Welcome, George")
}else
{
  println("What is your name?")
}

// Logical Operators

// AND &&
println((1==2) && (2==2))
println((1==1) && (2==2))

// OR ||
println((1==2) || (2==2))

// NOT !
println(!(1==1))
