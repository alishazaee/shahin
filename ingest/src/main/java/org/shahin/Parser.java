package org.shahin;

import java.util.Optional;

public interface Parser {
    Optional<byte[]> encode(String line);
}
