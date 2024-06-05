using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Exercise
{
    class Program
    {
        class Animal
        {
            public string Name { get; private set; }    // 名前
            public int Age { get; private set; }         // 年齢
            public Animal(string name, int age)
            {
                Name = name;
                Age = age;
            }
            public virtual void Speak()
            {
                Console.WriteLine("......");
            }
            public void ShowProfile()
            {
                Console.WriteLine(Name + "," + Age + "歳");
            }
        }
        class Dog : Animal 
        {
            public Dog(string name, int age) : base(name, age) { }

            public void Run() {
                Console.WriteLine("dadada");
            }
            public override void Speak()
            {
               Console.WriteLine("のっとってやったiso");   
            }

        }
        class Cat : Animal
        {
            public Cat(string name, int age) : base(name, age) { }            
            public void Sleep()
            {
                Console.WriteLine("スースー");
            }
        }

        static void Main()
        {
            Animal[] MyPets = new Animal[4];
            MyPets[0] = new Cat("たま", 3);
            MyPets[1] = new Dog("ぽち", 4);
            MyPets[2] = new Cat("ミケ", 4);
            MyPets[3] = new Dog("ジョン", 5);
            foreach (var item in MyPets)
            {
                Console.WriteLine(item);
                item.ShowProfile();
                item.Speak();
            }
        }
    }


}




