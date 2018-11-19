package io.wizzie.normalizer.base.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ConversionUtilsUnitTest {

    @Test
    public void conversionFromStringToLongObjectIsCorrectTest() {
        String number = "1234567890";

        assertEquals(new Long(1234567890L), ConversionUtils.toLong(number));
    }

    @Test
    public void conversionFromIntegerToLongObjectIsCorrectTest() {
        int number = 1234567890;

        assertEquals(new Long(1234567890L), ConversionUtils.toLong(number));
    }

    @Test
    public void conversionFromLongToLongObjectIsCorrectTest() {
        long number = 1234567890L;

        assertEquals(new Long(number), ConversionUtils.toLong(number));
    }

    @Test
    public void conversionFromDoubleToLongObjectIsCorrectTest() {
        double number = 1234567890.9;

        assertEquals(new Long(1234567890L), ConversionUtils.toLong(number));
    }

    @Test
    public void conversionFromFloatToLongObjectIsCorrectTest() {
        float number = 123.8f;

        assertEquals(new Long(123L), ConversionUtils.toLong(number));
    }

    @Test
    public void notANumberException() {
        assertNull(ConversionUtils.toLong("ABC"));
    }

}
