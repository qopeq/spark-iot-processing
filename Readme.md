https://github.com/qopeq/spark-iot-processing
# Spark event streaming

Aplikacja do obsługi wydarzeń z pojazdów

* Wzorzec projektowy - metoda wytwórcza/factory method
* Uczestnicy:

    * EventProcessor - Product - klasa abstrakcyjna dla konkretnych produktów
        * process_event()
        * validate_event()
        * json_parser()

    * CarEventProcessor, MotorcycleEventProcessor, EvEventProcessor,FaultEventProcessor - ConcreteProducts - właściwy produkt

        * validate_event() - sprawdza czy event zawiera poprawne dane
        * json_parser() - parsuje surowy rekord/string do jsona i wybiera odpowienie, zależnie od rodzaju eventu, kolumny
        * process_event() - używa validate_event() oraz json_parser() a następnie zapisuje event w odpowiednim folderze

    * EventProcessorFactory - Creator / Factory
        * get_factory() - tworzy konkretne fabryki zależnie od argumentu
        * create_processor() - abstrakcja/tworzy konkretny processor 

    * CarProcessorFactory, MotorcycleProcessorFactory, EvProcessorFactory, FaultProcessorFactory - ConcreteCreators / ConcreteFactories - tworzy właściwy produkt
        * create_processor() - tworzy konkretny produkt
![alt text]([http://url/to/img.png](https://github.com/qopeq/spark-iot-processing/blob/main/Untitled%20Diagram.jpg))
