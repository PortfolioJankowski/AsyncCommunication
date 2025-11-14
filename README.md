🚀 Architektura Systemu Asynchronicznego (RabbitMQ + Transactional Outbox)
Niniejsza aplikacja implementuje wzorzec Request/Reply (Żądanie/Odpowiedź) dla operacji na dużych danych (blobach), wykorzystując Wzorzec Transactional Outbox oraz zaawansowaną konfigurację RabbitMQ z mechanizmem Dead Letter Queue (DLQ), aby zapewnić niezawodność i trwałość danych.

1. 🌐 Przepływ Komunikacji (Flow)Aplikacja składa się z dwóch głównych serwisów w tle (BackgroundService) komunikujących się asynchronicznie przez RabbitMQ
1.1. 📩 Faza Żądania (Request)Producer (Klient/API): Inicjuje żądanie (np. generowanie raportu). Generuje unikalny CorrelationId i wysyła wiadomość do kolejki żądań: blob_requests_queue. Następnie czeka na odpowiedź.

1.2. 👷 Consumer 1: OutboxService (Worker)
Ten serwis odpowiada za obsługę żądania i transakcyjny zapis danych.
Odbiór: Konsumuje wiadomość z blob_requests_queue.
Transakcyjny Zapis: Rozpoczyna transakcję bazodanową (SQLite). 
W ramach atomowej operacji: Zapisuje wynik (dane) do tabeli Blobs.
Zapisuje rekord do tabeli Outboxes (IsProcessed = false), przechowując kluczowy CorrelationId.
Potwierdzenie (Ack): Dopiero po pomyślnym zapisie do obu tabel (w ramach transakcji), wysyła BasicAck do RabbitMQ.
Niezawodność: Jeśli transakcja DB się nie powiedzie, wysyłany jest BasicNack, a wiadomość trafia do DLQ (patrz sekcja 3).

1.3. 📢 Consumer 2: OutboxPublisherService (Publisher)Ten serwis odpowiada za niezawodne wysyłanie powiadomień po zapisie danych.
Cykliczne Odpytywanie: Serwis działa w pętli, używając IServiceScopeFactory do tworzenia nowego, krótkotrwałego zakresu DI dla każdej operacji DB. Odpytuje tabelę Outboxes, szukając rekordów z IsProcessed = false.
Publikacja Odpowiedzi: Pobiera ID Bloba i CorrelationId. Wysyła wiadomość do kolejki odpowiedzi (blob_responses_queue).Finalizacja: Oznacza rekord w tabeli Outboxes jako przetworzony (IsProcessed = true).

1.4. ↩️ Faza Odpowiedzi (Reply)Klient: Odbiera wiadomość z kolejki odpowiedzi. Używa wartości CorrelationId do odblokowania pierwotnie oczekującego zadania i informuje użytkownika o gotowości bloba.

2. 🔑 Kluczowe Komponenty Architektury
Rola w Systemie CorrelationId 
Identyfikator Transakcji. Łączy żądanie A z odpowiedzią B.
Zapewnia, że klient wie, która odpowiedź jest dla którego żądania.

IDbRepository Warstwa Danych. 
Encapsuluje operacje na SQLite, dbając o transakcyjność zapisu (Blob + Outbox).

IServiceScopeFactory Zarządzanie Cyklem Życia. 
Wstrzykiwany do Singletonowych serwisów tła, umożliwia bezpieczne tworzenie krótkotrwałych (Scoped) połączeń z bazą danych dla każdej operacji.

Redis Warstwa Cache. 
Służy do przyspieszenia odczytu blobów. 

CachedDbRepositoryDecorator zapisuje blob do Redis po jego utworzeniu (Write-Through), redukując obciążenie SQLite przy ponownym dostępie.

3. 💣 Mechanizm Dead Letter Queue (DLQ)
System jest skonfigurowany do obsługi błędów konsumpcji za pomocą kaskady DLQ, zapewniającej trwałość nieprzetworzonych wiadomości.
Źródło Problemu: Wiadomość w blob_requests_queue jest przetwarzana przez OutboxService i z jakiegoś powodu zostaje odrzucona (BasicNack z requeue: false), np. przez błąd walidacji, przejściowy błąd DB, lub celowe odrzucenie.DLX (Dead Letter Exchange): 
Odrzucona wiadomość jest kierowana do dedykowanego Dead Letter Exchange (dlx_exchange).
DLQ (Dead Letter Queue): Stamtąd trafia do kolejki martwych wiadomości (dead_letter_queue). 
Ta kolejka jest skonfigurowana z:TTL (Time To Live): Wiadomość jest przetrzymywana, np. przez 24 godziny.
Kolejnym DLX: Po wygaśnięciu TTL, wiadomość jest automatycznie kierowana do następnego Exchange.Dead End Point (Ostateczne Archiwum): Po upływie TTL, wiadomość trafia do finalnej kolejki (dead_end_point_queue). 
Jest to ostateczne archiwum, które może być monitorowane lub konsumowane przez zewnętrzny serwis audytowy, gwarantując, że żadna wiadomość nie zostanie utracona, choć może wymagać ręcznej interwencji.
