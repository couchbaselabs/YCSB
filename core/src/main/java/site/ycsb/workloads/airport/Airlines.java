/**
 * Copyright (c) 2023-2026 benchANT GmbH. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.workloads.airport;

import java.util.List;

import site.ycsb.generator.DiscreteGenerator;

import java.util.LinkedList;

final class Airline {
    final String name;
    final String code;
    Airline(String code, String name) {
        this.name = name;
        this.code = code;
    }
}
final class AirlineListBuilder {
    private final List<Airline> airlines = new LinkedList<>();
    AirlineListBuilder addAirline(String iata, String name) {
        airlines.add(new Airline(iata, name));
        return this;
    }
    Airline[] build() {
        return airlines.toArray(new Airline[airlines.size()]);
    }
}

final class Airlines {
    public final static Airline[] ALL_AIRLINES = new AirlineListBuilder()
        .addAirline("QH", "Air Florida")
        .addAirline("RV", "Air Canada Rouge")
        .addAirline("EV", "Atlantic Southeast Airlines")
        .addAirline("HP", "America West Airlines")
        .addAirline("AA", "American Airlines")
        .addAirline("GQ", "Big Sky Airlines")
        .addAirline("4B", "Boutique Air")
        .addAirline("L9", "Bristow U.S. LLC")
        .addAirline("E9", "Boston-Maine Airways")
        .addAirline("C6", "CanJet")
        .addAirline("W2", "Canadian Western Airlines")
        .addAirline("9M", "Central Mountain Air")
        .addAirline("QE", "Crossair Europe")
        .addAirline("DK", "Eastland Air")
        .addAirline("MB", "Execair Aviation")
        .addAirline("OW", "Executive Airlines")
        .addAirline("EV", "ExpressJet")
        .addAirline("7F", "First Air")
        .addAirline("F8", "Flair Airlines")
        .addAirline("PA", "Florida Coastal Airlines")
        .addAirline("RF", "Florida West International Airways")
        .addAirline("F7", "Flybaboo")
        .addAirline("ST", "Germania")
        .addAirline("4U", "Germanwings")
        .addAirline("HF", "Hapagfly")
        .addAirline("HB", "Harbor Airlines")
        .addAirline("HQ", "Harmony Airways")
        .addAirline("HA", "Hawaiian Airlines")
        .addAirline("2S", "Island Express")
        .addAirline("QJ", "Jet Airways")
        .addAirline("PP", "Jet Aviation")
        .addAirline("3K", "Jetstar Asia Airways")
        .addAirline("B6", "JetBlue Airways")
        .addAirline("0J", "Jetclub")
        .addAirline("LT", "LTU International")
        .addAirline("LH", "Lufthansa")
        .addAirline("CL", "Lufthansa CityLine")
        .addAirline("L1", "Lufthansa Systems")
        .addAirline("L2", "Lynden Air Cargo")
        .addAirline("Y9", "Lynx Air")
        .addAirline("L4", "Lynx Aviation")
        .addAirline("BF", "MarkAir")
        .addAirline("CC", "Macair Airlines")
        .addAirline("YV", "Mesa Airlines")
        .addAirline("XJ", "Mesaba Airlines")
        .addAirline("LL", "Miami Air International")
        .addAirline("OL", "OLT Express Germany")
        .addAirline("8P", "Pacific Coastal Airlines")
        .addAirline("LW", "Pacific Wings")
        .addAirline("KS", "Peninsula Airways")
        .addAirline("HP", "Phoenix Airways")
        .addAirline("QF", "Qantas")
        .addAirline("RW", "Republic Airways")
        .addAirline("V2", "Vision Airlines")
        .addAirline("7S", "Ryan Air Services")
        .addAirline("GG", "Sky Lease Cargo")
        .addAirline("SH", "Sharp Airlines")
        .addAirline("KV", "Sky Regional Airlines")
        .addAirline("BB", "Seaborne Airlines")
        .addAirline("SQ", "Singapore Airlines Cargo")
        .addAirline("LX", "Swiss International Air Lines")
        .addAirline("XG", "Sunexpress Deutschland")
        .addAirline("X3", "TUI fly Deutschland")
        .addAirline("VX", "Virgin America")
        .addAirline("W7", "Western Pacific Airlines")
        .addAirline("3J", "Zip")
        .build();
    static String nameForKey(String key) {
        for(Airline a : ALL_AIRLINES) {
            if(a.code.equals(key)) {
                return a.name;
            }
        }
        return null;
    }
    static DiscreteGenerator createAirlinesGenerator() {
        final double uniformDistribution = 1 / ((double) ALL_AIRLINES.length);
        final DiscreteGenerator generator = new DiscreteGenerator();
        for(Airline a : ALL_AIRLINES) {
            generator.addValue(uniformDistribution, a.code);
        }
        return generator;
    }
    private Airlines() {}
}
