/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions.hilbert;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;

import javax.imageio.ImageIO;

import org.apache.spark.sql.catalyst.expressions.hilbert.exceptions.IORuntimeException;


public final class HilbertCurveRenderer {

    private HilbertCurveRenderer() {
        // prevent instantiation
    }

    public static void renderToFile(int bits, int width, String filename, Option... options) {
        BufferedImage b = render(bits, width, options);
        try {
            ImageIO.write(b, "PNG", new File(filename));
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
    }

    public static enum Option {
        COLORIZE, LABEL;
    }

    public static BufferedImage render(int bits, int width, Option... options) {
        int dimensions = 2;
        HilbertCurve c = HilbertCurve.bits(bits).dimensions(dimensions);
        int n = 1 << bits;
        int height = width;
        BufferedImage b = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = b.createGraphics();
        g.setBackground(Color.white);
        g.fillRect(0, 0, width, height);
        g.setPaint(Color.black);
        g.setStroke(new BasicStroke(0.5f));
        int margin = 10;
        int cellSize = (width - 2 * margin) / (n);

        if (contains(options, Option.COLORIZE)) {
            int x = margin + cellSize / 2;
            int y = margin + cellSize / 2;
            for (long i = 0; i < n * n; i++) {
                fill(n, g, cellSize, x, y, i);
                long[] point = c.point(BigInteger.valueOf(i));
                int x2 = (int) Math.round((double) point[0] / (n - 1) * (width - 2 * margin - cellSize) + margin)
                        + cellSize / 2;
                int y2 = (int) Math.round((double) point[1] / (n - 1) * (height - 2 * margin - cellSize) + margin)
                        + cellSize / 2;
                x = x2;
                y = y2;
            }
            fill(n, g, cellSize, x, y, n * n);
        }
        if (contains(options, Option.LABEL)) {
            int x = margin + cellSize / 2;
            int y = margin + cellSize / 2;
            x = margin + cellSize / 2;
            y = margin + cellSize / 2;
            g.setColor(Color.black);
            for (long i = 0; i < n * n; i++) {
                long[] point = c.point(BigInteger.valueOf(i));
                int x2 = (int) Math.round((double) point[0] / (n - 1) * (width - 2 * margin - cellSize) + margin)
                        + cellSize / 2;
                int y2 = (int) Math.round((double) point[1] / (n - 1) * (height - 2 * margin - cellSize) + margin)
                        + cellSize / 2;
                x = x2;
                y = y2;
                drawNumber(g, x, y, i);
            }
        }

        int x = margin + cellSize / 2;
        int y = margin + cellSize / 2;
        x = margin + cellSize / 2;
        y = margin + cellSize / 2;
        g.setColor(Color.black);
        for (long i = 0; i < n * n; i++) {
            long[] point = c.point(BigInteger.valueOf(i));
            int x2 = (int) Math.round((double) point[0] / (n - 1) * (width - 2 * margin - cellSize) + margin)
                    + cellSize / 2;
            int y2 = (int) Math.round((double) point[1] / (n - 1) * (height - 2 * margin - cellSize) + margin)
                    + cellSize / 2;
            g.drawLine(x, y, x2, y2);
            x = x2;
            y = y2;
        }
        return b;
    }

    private static boolean contains(Option[] options, Option option) {
        for (Option o : options) {
            if (o == option) {
                return true;
            }
        }
        return false;
    }

    private static void drawNumber(Graphics2D g, int x, int y, long i) {
        g.drawString(i + "", x + 2, y - 2);
    }

    private static void fill(int n, Graphics2D g, int cellSize, int x, int y, long i) {
        Color color = Color.getHSBColor(((float) i) / n / n, 0.5f, 1.0f);
        g.setColor(color);
        g.fillRect(x - cellSize / 2, y - cellSize / 2, cellSize + 1, cellSize + 1);
    }
}
