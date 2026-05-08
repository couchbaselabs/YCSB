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

final class Airplane {
    final String name;
    final String code;
    Airplane(String code, String name) {
        this.name = name;
        this.code = code;
    }
}
final class AirplaneListBuilder {
    private final List<Airplane> Airplanes = new LinkedList<>();
    AirplaneListBuilder addAirplane(String iata, String name) {
        Airplanes.add(new Airplane(iata, name));
        return this;
    }
    Airplane[] build() {
        return Airplanes.toArray(new Airplane[Airplanes.size()]);
    }
}

final class Airplanes {
    public final static Airplane[] ALL_AIRCRAFTS = new AirplaneListBuilder()
        .addAirplane("A318", "Airbus A318")
        .addAirplane("B37M", "Boeing 737 MAX 7")
        .addAirplane("B734", "Boeing 737-400")
        .addAirplane("C208", "Cessna 208 Caravan")
        .addAirplane("CL2T", "Bombardier 415")
        .addAirplane("D228", "Dornier 228")
        .addAirplane("DC93", "Douglas DC-9-30")
        .addAirplane("WW24", "Israel Aircraft Industries 1124 Westwind")
        .addAirplane("A388", "Airbus A380-800")
        .addAirplane("C130", "\tLockheed L-182 / 282 / 382 (L-100) Hercules")
        .build();
    static DiscreteGenerator createAircraftGenerator() {
        final double uniformDistribution = 1 / ((double) ALL_AIRCRAFTS.length);
        final DiscreteGenerator generator = new DiscreteGenerator();
        for(Airplane a : ALL_AIRCRAFTS) {
            generator.addValue(uniformDistribution, a.code);
        }
        return generator;
    }
    private Airplanes() {}
}
