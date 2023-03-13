val sayHello: String = "Hello";

var fruitsList  = List("Orange", "Mango", "Grapes", "Apple")

for (x <- fruitsList) {
  println(x)
}

var reversedList = fruitsList.reverse

println(reversedList)

var backwardsFruits = fruitsList.map((fruit: String) => {fruit.reverse})