class gadget:
    def __init__(self,brand,model):
        self.brand= brand
        self.model= model
    def display_info(self):
        print(f"brand:{self.brand},model:{self.model}")
class laptop(gadget):
     def __init__(self, brand, model, year):
         super().__init__(brand, model)
         self.year=year
     def display_laptop_info(self):
         print(f"The Brand of the laptop is {self.brand} {self.model} {self.year}")
my_laptop =laptop("Lenovo","Thinkpad",2000)
my_laptop.display_laptop_info()