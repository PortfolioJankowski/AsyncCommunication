var publisher = new Producer();
string input = "I wanna BLOB!";
string result = await publisher.Run(input);


Console.ReadLine();