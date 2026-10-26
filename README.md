# Taktik Mesaj Ağ Geçidi (TMG)

[![ci](https://github.com/mustafagulturk/taktik-mesaj-gecidi/actions/workflows/ci.yml/badge.svg)](https://github.com/mustafagulturk/taktik-mesaj-gecidi/actions/workflows/ci.yml)

Sabit bit düzenli ikili taktik mesajları (konum raporu, temas raporu, emir,
serbest metin…) kodlayan/çözen ve bunları TCP üzerinden sahadaki birliklere
dağıtan bir ağ geçidi sunucusu. Bozuk paketlere, kopan bağlantılara ve
gecikmeye karşı dayanıklı olacak şekilde tasarlandı; kodek ve akış ayrıştırıcı
Go'nun yerleşik fuzz altyapısıyla test ediliyor.

Yalnızca Go standart kütüphanesi kullanılıyor, harici bağımlılık yok.

```
 birlik 10 ──┐                        ┌── birlik 20
 birlik 11 ──┼──TCP──▶  gateway  ◀──TCP──┼── birlik 21
 birlik 12 ──┘   (çerçeve, CRC,       └── birlik 22
                  yönlendirme, ACK)
```

## Öne çıkanlar

- **Bit düzeyinde protokol** — her alan tam ihtiyaç duyduğu bit sayısıyla
  paketleniyor (enlem 24 bit, boylam 25 bit, rota 9 bit, saat 17 bit…). Bir
  konum raporu çerçevesi toplam **26 bayt**.
- **Kendini yeniden senkronlayan akış okuyucu** — çöp bayt, bozuk uzunluk
  alanı veya CRC hatası durumunda okuyucu SYNC kelimesini yeniden arar;
  bozuk bir çerçeve arkasındaki sağlam çerçeveyi asla yutmaz.
- **Katı doğrulama** — sürüm, tür, ayrılmış bitler, türe göre yük uzunluğu,
  alan aralıkları (saat < 86400, rota < 360 …) ve CRC-16/CCITT ayrı ayrı
  kontrol ediliyor. Kodlama kanonik: çözülen her çerçeve bayt bayt aynı
  şekilde yeniden kodlanıyor (fuzz ile doğrulanıyor).
- **Öncelik sıralı kuyruk** — her oturumun FLASH > IMMEDIATE > PRIORITY >
  ROUTINE sıralı, sınırlı bir giden kuyruğu var. Kuyruk dolduğunda önce düşük
  öncelikli trafik atılıyor; ROUTINE yığını FLASH mesajını asla bekletmiyor.
- **Yavaş istemci izolasyonu** — her bağlantının kendi okuma/yazma
  goroutine'i ve yazma zaman aşımı var; okumayan bir birlik diğerlerini
  yavaşlatamıyor.
- **En fazla bir kez teslim** — (kaynak, sıra no) çiftleri sunucu genelinde
  TTL'li bir pencerede tutuluyor. İstemci ACK alamazsa aynı sıra numarasıyla
  yeniden gönderiyor; bağlantı kopup yeniden kurulsa bile alıcı mesajı bir kez
  görüyor.
- **İki yönlü canlılık** — sunucu belirli süre geçerli çerçeve gelmeyen
  oturumu kapatıyor; istemci de heartbeat ACK'leri gelmezse sunucuyu ölü
  sayıp üstel geri çekilme + jitter ile yeniden bağlanıyor.
- **Bağlantı devralma** — aynı birlik yeniden bağlandığında eski (yarı açık)
  oturum hemen kapatılıyor.
- **Saldırgan eşlere karşı** — yalnızca çöp gönderen bağlantı çöp bütçesi
  dolunca düşürülüyor; kaynak adresi taklit eden çerçeveler reddediliyor.
- **Kaos proxy'si** — gecikme, jitter, bit bozma, parça düşürme,
  parçalama (fragmentation) ve TCP RST ile bağlantı koparma uygulayan bir TCP
  proxy'si. Uçtan uca testler bu proxy üzerinden koşuyor.

## Tel formatı

Tüm alanlar big-endian, en anlamlı bit önce (MSB-first).

```
+--------+----------------------------------------------+---------+--------+
| SYNC   | BAŞLIK (72 bit)                              | YÜK     | CRC16  |
| 0xC35A | ver:3 type:5 pri:2 cls:2 ack:1 rsv:3         | 0..255  | CCITT  |
| 16 bit | src:16 dst:16 seq:16 len:8                   | bayt    | 16 bit |
+--------+----------------------------------------------+---------+--------+
```

| Alan  | Bit | Açıklama |
|-------|-----|----------|
| ver   | 3   | Protokol sürümü (1) |
| type  | 5   | Mesaj türü |
| pri   | 2   | 0 ROUTINE, 1 PRIORITY, 2 IMMEDIATE, 3 FLASH |
| cls   | 2   | 0 TASNİF DIŞI, 1 HİZMETE ÖZEL, 2 GİZLİ, 3 ÇOK GİZLİ |
| ack   | 1   | Alındı onayı isteniyor |
| rsv   | 3   | Ayrılmış, sıfır olmalı |
| src   | 16  | Gönderen birlik (0x0000 = ağ geçidi) |
| dst   | 16  | Alıcı birlik (0xFFFF = herkese yayın) |
| seq   | 16  | Gönderenin sıra numarası |
| len   | 8   | Yük uzunluğu (türe göre doğrulanır) |

CRC-16/CCITT-FALSE (poly 0x1021, init 0xFFFF) başlık + yük üzerinden
hesaplanır. Tek bit hatalarının tamamı ve 16 bite kadar tüm patlama hataları
yakalanır.

### Mesaj türleri

| Tür | Kod | Yük | Bit düzeni |
|-----|-----|-----|------------|
| HEARTBEAT | 1 | 4 B  | `time:17 status:4 battery:7 rsv:4` |
| POSREP    | 2 | 13 B | `time:17 lat:24 lon:25 alt:16 speed:10 heading:9 status:3` |
| CONTACT   | 3 | 13 B | `time:17 lat:24 lon:25 category:4 count:7 heading:9 speed:10 confidence:3 rsv:5` |
| ORDER     | 4 | 12 B | `time:17 code:6 target_lat:24 target_lon:25 execute_at:17 rsv:7` |
| ACK       | 5 | 3 B  | `acked_seq:16 status:3 rsv:5` |
| FREETEXT  | 6 | değişken | `len:6` + `len × 6 bit karakter` (sıfırla doldurulur) |

- `time`: UTC gece yarısından itibaren saniye (0..86399).
- `lat`: işaretli, 1 LSB = 90/2²³° (~1,2 m). `lon`: işaretli, 1 LSB = 180/2²⁴°.
- FREETEXT 64 sembollü 6 bitlik bir alfabe kullanır (büyük harf, rakam,
  noktalama). Türkçe karakterler gönderimde katlanır: `Düşman` → `DUSMAN`.

Örnek — `tmcli encode` ile üretilmiş bir konum raporu:

```
c35a 22 80 000a 0014 0001 0d  475e1c635a8bae7440ea82a870  b2b4
SYNC │  │  src  dst  seq  len  yük (13 bayt)               CRC
     │  └ 1000 0000: pri=IMMEDIATE cls=0 ack=0 rsv=0
     └ 0010 0010: ver=1 type=2 (POSREP)
```

## Mimari

```
protocol/        çerçeve kodlama/çözme, mesaj bit düzenleri, akış okuyucu
internal/bitio/  MSB-first bit yazıcı/okuyucu
gateway/         sunucu: oturumlar, yönlendirme, öncelik kuyruğu, tekrar filtresi
client/          birlik istemcisi: yeniden bağlanma, heartbeat, ACK/yeniden gönderim
chaos/           hata enjekte eden TCP proxy
e2e/             kaos proxy'si üzerinden uçtan uca testler
cmd/gateway      sunucu (TCP + /metrics, /units, /healthz HTTP uçları)
cmd/tmcli        komut satırı istemcisi (listen, send, encode, decode)
cmd/chaosproxy   kaos proxy'si komut satırı aracı
```

Her oturumda iki goroutine var: okuyucu (çöz → doğrula → yönlendir) ve yazıcı
(öncelik kuyruğunu boşalt → toplu yaz → flush). Yönlendirici hiçbir zaman bir
soket yazmasını beklemez; yalnızca hedef oturumun kuyruğuna bırakır.

ACK durumları: `DELIVERED` (hedef oturum(lar)ın kuyruğuna alındı), `NO_ROUTE`,
`DUPLICATE` (daha önce alınmıştı, tekrar iletilmedi), `REJECTED`,
`QUEUE_FULL`.

## Çalıştırma

```bash
make build                      # bin/gateway, bin/tmcli, bin/chaosproxy

bin/gateway -listen :7400 -http 127.0.0.1:7401
bin/tmcli listen -unit 20
bin/tmcli send -unit 10 -to 20 -type posrep -lat 39.92077 -lon 32.85411 -heading 270 -speed 42 -prio immediate
bin/tmcli send -unit 10 -to all -type text -text "Tüm birlikler hazır olda" -prio flash -class secret
bin/tmcli send -unit 10 -to 20 -type order -order advance -target-lat 41.01 -target-lon 28.97 -at 300
curl -s 127.0.0.1:7401/metrics
```

Kötü bir ağı canlı denemek için:

```bash
bin/chaosproxy -listen 127.0.0.1:7500 -target 127.0.0.1:7400 \
  -latency 80ms -jitter 40ms -corrupt 0.001 -chunk 5 -reset 5s
bin/tmcli send -addr 127.0.0.1:7500 -unit 10 -to 20 -type contact \
  -lat 37.0 -lon 35.3 -category armor -count 4 -v
```

Docker:

```bash
docker build -t tmg .
docker run -p 7400:7400 -p 7401:7401 tmg
```

## Testler

```bash
make test        # birim + entegrasyon + uçtan uca testler
make race        # -race ile
make fuzz        # tüm fuzz hedefleri (FUZZTIME=30s)
make bench
```

Fuzz hedefleri:

| Hedef | Ne doğruluyor |
|-------|---------------|
| `FuzzDecode` | Rastgele bayt dizisi asla panik üretmiyor; çözülebilen her çerçeve bayt bayt aynı yeniden kodlanıyor |
| `FuzzStreamReader` | Rastgele akışta panik/sonsuz döngü yok; dönen her çerçeve girdide gerçekten var |
| `FuzzStreamResync` | Geçerli çerçeve dizisinin bir kısmı bozulduğunda, dokunulmamış çerçevelerin hepsi sırasıyla kurtarılıyor |
| `FuzzFrameRoundTrip` | Rastgele alan değerleriyle kurulan mesajlar kodla → çöz sonrasında birebir aynı |
| `FuzzRoundTrip` (bitio) | 1..64 bit genişliğindeki işaretli/işaretsiz alanlar kayıpsız |

Uçtan uca testlerden `TestExactlyOnceOverHostileUplink`, gönderen birliği
bit bozan, yazmaları parçalayan, gecikme ekleyen ve bağlantıyı birkaç yüz
milisaniyede bir RST ile koparan proxy'nin arkasına koyar; her emrin ACK
almasını ve alıcının her emri **tam olarak bir kez** görmesini doğrular.

## Lisans

MIT
