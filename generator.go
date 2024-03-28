package main

import (
	"bytes"
	"context"
	"fmt"
	"golang.org/x/sync/semaphore"
	"math"
	"math/rand"
	"onebrc/beauty"
	"os"
	"sync"
)

type GenerateConfig struct {
	output       string
	records      int
	maxChunkSize int
	workers      int
}

func (c GenerateConfig) chunkSize() int {
	return min(c.records, c.maxChunkSize)
}

func (c GenerateConfig) totalChunks() int {
	return c.records / c.chunkSize()
}

func generate(config GenerateConfig) {
	queue := make(chan interval)

	pb := beauty.NewProgressBar(config.totalChunks())
	go func() {
		defer close(queue)

		for i := 0; i < config.totalChunks(); i++ {
			start := i * config.chunkSize()
			end := min(config.records, i*config.chunkSize()+config.chunkSize())

			queue <- interval{start, end}
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(config.workers)

	mutex := sync.Mutex{}
	slots := semaphore.NewWeighted(int64(10))

	for i := 0; i < config.workers; i++ {
		go func() {
			defer wg.Done()
			buffer := bytes.Buffer{}

			for interval := range queue {
				processInterval(interval, buffer, config.output, slots, &mutex)
				pb.Increment()
			}
		}()
	}
	wg.Wait()
}

func processInterval(
	interval interval,
	buffer bytes.Buffer,
	filename string,
	slots *semaphore.Weighted,
	mutex *sync.Mutex,
) {
	rnd := rand.New(rand.NewSource(int64(interval.start)))

	for i := interval.start; i < interval.end; i++ {
		index := rnd.Int63() % int64(len(stations))
		temperature := rnd.Float32() * 100
		sign := float32(math.Copysign(1, rnd.NormFloat64()))

		buffer.WriteString(stations[index])
		buffer.WriteString(";")
		buffer.WriteString(fmt.Sprintf("%.1f", temperature*sign))
		buffer.WriteString("\n")
	}

	just(0, slots.Acquire(context.Background(), 1))
	defer slots.Release(1)

	file := just(os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644))
	defer file.Close()

	mutex.Lock()
	defer mutex.Unlock()

	just(file.Write(buffer.Bytes()))

	buffer.Reset()
}

func just[T any](result T, err error) T {
	if err != nil {
		fmt.Printf("An error occured duting processing [%s]", err)
		panic(err)
	}
	return result
}

var stations = []string{
	"Abha",
	"Abidjan",
	"Abéché",
	"Accra",
	"Addis Ababa",
	"Adelaide",
	"Aden",
	"Ahvaz",
	"Albuquerque",
	"Alexandra",
	"Alexandria",
	"Algiers",
	"Alice Springs",
	"Almaty",
	"Amsterdam",
	"Anadyr",
	"Anchorage",
	"Andorra la Vella",
	"Ankara",
	"Antananarivo",
	"Antsiranana",
	"Arkhangelsk",
	"Ashgabat",
	"Asmara",
	"Assab",
	"Astana",
	"Athens",
	"Atlanta",
	"Auckland",
	"Austin",
	"Baghdad",
	"Baguio",
	"Baku",
	"Baltimore",
	"Bamako",
	"Bangkok",
	"Bangui",
	"Banjul",
	"Barcelona",
	"Bata",
	"Batumi",
	"Beijing",
	"Beirut",
	"Belgrade",
	"Belize City",
	"Benghazi",
	"Bergen",
	"Berlin",
	"Bilbao",
	"Birao",
	"Bishkek",
	"Bissau",
	"Blantyre",
	"Bloemfontein",
	"Boise",
	"Bordeaux",
	"Bosaso",
	"Boston",
	"Bouaké",
	"Bratislava",
	"Brazzaville",
	"Bridgetown",
	"Brisbane",
	"Brussels",
	"Bucharest",
	"Budapest",
	"Bujumbura",
	"Bulawayo",
	"Burnie",
	"Busan",
	"Cabo San Lucas",
	"Cairns",
	"Cairo",
	"Calgary",
	"Canberra",
	"Cape Town",
	"Changsha",
	"Charlotte",
	"Chiang Mai",
	"Chicago",
	"Chihuahua",
	"Chișinău",
	"Chittagong",
	"Chongqing",
	"Christchurch",
	"City of San Marino",
	"Colombo",
	"Columbus",
	"Conakry",
	"Copenhagen",
	"Cotonou",
	"Cracow",
	"Da Lat",
	"Da Nang",
	"Dakar",
	"Dallas",
	"Damascus",
	"Dampier",
	"Dar es Salaam",
	"Darwin",
	"Denpasar",
	"Denver",
	"Detroit",
	"Dhaka",
	"Dikson",
	"Dili",
	"Djibouti",
	"Dodoma",
	"Dolisie",
	"Douala",
	"Dubai",
	"Dublin",
	"Dunedin",
	"Durban",
	"Dushanbe",
	"Edinburgh",
	"Edmonton",
	"El Paso",
	"Entebbe",
	"Erbil",
	"Erzurum",
	"Fairbanks",
	"Fianarantsoa",
	"Flores,  Petén",
	"Frankfurt",
	"Fresno",
	"Fukuoka",
	"Gabès",
	"Gaborone",
	"Gagnoa",
	"Gangtok",
	"Garissa",
	"Garoua",
	"George Town",
	"Ghanzi",
	"Gjoa Haven",
	"Guadalajara",
	"Guangzhou",
	"Guatemala City",
	"Halifax",
	"Hamburg",
	"Hamilton",
	"Hanga Roa",
	"Hanoi",
	"Harare",
	"Harbin",
	"Hargeisa",
	"Hat Yai",
	"Havana",
	"Helsinki",
	"Heraklion",
	"Hiroshima",
	"Ho Chi Minh City",
	"Hobart",
	"Hong Kong",
	"Honiara",
	"Honolulu",
	"Houston",
	"Ifrane",
	"Indianapolis",
	"Iqaluit",
	"Irkutsk",
	"Istanbul",
	"İzmir",
	"Jacksonville",
	"Jakarta",
	"Jayapura",
	"Jerusalem",
	"Johannesburg",
	"Jos",
	"Juba",
	"Kabul",
	"Kampala",
	"Kandi",
	"Kankan",
	"Kano",
	"Kansas City",
	"Karachi",
	"Karonga",
	"Kathmandu",
	"Khartoum",
	"Kingston",
	"Kinshasa",
	"Kolkata",
	"Kuala Lumpur",
	"Kumasi",
	"Kunming",
	"Kuopio",
	"Kuwait City",
	"Kyiv",
	"Kyoto",
	"La Ceiba",
	"La Paz",
	"Lagos",
	"Lahore",
	"Lake Havasu City",
	"Lake Tekapo",
	"Las Palmas de Gran Canaria",
	"Las Vegas",
	"Launceston",
	"Lhasa",
	"Libreville",
	"Lisbon",
	"Livingstone",
	"Ljubljana",
	"Lodwar",
	"Lomé",
	"London",
	"Los Angeles",
	"Louisville",
	"Luanda",
	"Lubumbashi",
	"Lusaka",
	"Luxembourg City",
	"Lviv",
	"Lyon",
	"Madrid",
	"Mahajanga",
	"Makassar",
	"Makurdi",
	"Malabo",
	"Malé",
	"Managua",
	"Manama",
	"Mandalay",
	"Mango",
	"Manila",
	"Maputo",
	"Marrakesh",
	"Marseille",
	"Maun",
	"Medan",
	"Mek'ele",
	"Melbourne",
	"Memphis",
	"Mexicali",
	"Mexico City",
	"Miami",
	"Milan",
	"Milwaukee",
	"Minneapolis",
	"Minsk",
	"Mogadishu",
	"Mombasa",
	"Monaco",
	"Moncton",
	"Monterrey",
	"Montreal",
	"Moscow",
	"Mumbai",
	"Murmansk",
	"Muscat",
	"Mzuzu",
	"N'Djamena",
	"Naha",
	"Nairobi",
	"Nakhon Ratchasima",
	"Napier",
	"Napoli",
	"Nashville",
	"Nassau",
	"Ndola",
	"New Delhi",
	"New Orleans",
	"New York City",
	"Ngaoundéré",
	"Niamey",
	"Nicosia",
	"Niigata",
	"Nouadhibou",
	"Nouakchott",
	"Novosibirsk",
	"Nuuk",
	"Odesa",
	"Odienné",
	"Oklahoma City",
	"Omaha",
	"Oranjestad",
	"Oslo",
	"Ottawa",
	"Ouagadougou",
	"Ouahigouya",
	"Ouarzazate",
	"Oulu",
	"Palembang",
	"Palermo",
	"Palm Springs",
	"Palmerston North",
	"Panama City",
	"Parakou",
	"Paris",
	"Perth",
	"Petropavlovsk-Kamchatsky",
	"Philadelphia",
	"Phnom Penh",
	"Phoenix",
	"Pittsburgh",
	"Podgorica",
	"Pointe-Noire",
	"Pontianak",
	"Port Moresby",
	"Port Sudan",
	"Port Vila",
	"Port-Gentil",
	"Portland (OR)",
	"Porto",
	"Prague",
	"Praia",
	"Pretoria",
	"Pyongyang",
	"Rabat",
	"Rangpur",
	"Reggane",
	"Reykjavík",
	"Riga",
	"Riyadh",
	"Rome",
	"Roseau",
	"Rostov-on-Don",
	"Sacramento",
	"Saint Petersburg",
	"Saint-Pierre",
	"Salt Lake City",
	"San Antonio",
	"San Diego",
	"San Francisco",
	"San Jose",
	"San José",
	"San Juan",
	"San Salvador",
	"Sana'a",
	"Santo Domingo",
	"Sapporo",
	"Sarajevo",
	"Saskatoon",
	"Seattle",
	"Ségou",
	"Seoul",
	"Seville",
	"Shanghai",
	"Singapore",
	"Skopje",
	"Sochi",
	"Sofia",
	"Sokoto",
	"Split",
	"St. John's",
	"St. Louis",
	"Stockholm",
	"Surabaya",
	"Suva",
	"Suwałki",
	"Sydney",
	"Tabora",
	"Tabriz",
	"Taipei",
	"Tallinn",
	"Tamale",
	"Tamanrasset",
	"Tampa",
	"Tashkent",
	"Tauranga",
	"Tbilisi",
	"Tegucigalpa",
	"Tehran",
	"Tel Aviv",
	"Thessaloniki",
	"Thiès",
	"Tijuana",
	"Timbuktu",
	"Tirana",
	"Toamasina",
	"Tokyo",
	"Toliara",
	"Toluca",
	"Toronto",
	"Tripoli",
	"Tromsø",
	"Tucson",
	"Tunis",
	"Ulaanbaatar",
	"Upington",
	"Ürümqi",
	"Vaduz",
	"Valencia",
	"Valletta",
	"Vancouver",
	"Veracruz",
	"Vienna",
	"Vientiane",
	"Villahermosa",
	"Vilnius",
	"Virginia Beach",
	"Vladivostok",
	"Warsaw",
	"Washington, D.C.",
	"Wau",
	"Wellington",
	"Whitehorse",
	"Wichita",
	"Willemstad",
	"Winnipeg",
	"Wrocław",
	"Xi'an",
	"Yakutsk",
	"Yangon",
	"Yaoundé",
	"Yellowknife",
	"Yerevan",
	"Yinchuan",
	"Zagreb",
	"Zanzibar City",
	"Zürich",
}

type interval struct {
	start int
	end   int
}
