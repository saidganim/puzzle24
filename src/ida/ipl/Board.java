package ida.ipl;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Scanner;

/**
 * Class representing a particular position of the 24 puzzle.
 */
public final class Board implements Serializable {

	private static final long serialVersionUID = 5914825388221307541L;

	private static final class Position {
		int x;

		int y;
	}

	static final int NSQRT = 5; // the 24 puzzle is 5 x 5

	static final int NPUZZLE = NSQRT * NSQRT - 1;

	// property of the sliding tile puzzle
	static final int BRANCH_FACTOR = 4;

	// positions of all the tiles in the goal position
	private static Position[] goal = new Position[NPUZZLE + 1];

	// static initializer of goal
	static {
		for (int i = 0; i <= NPUZZLE; i++) {
			goal[i] = new Position();
		}

		int v = 0;
		for (int y = 0; y < NSQRT; y++) {
			for (int x = 0; x < NSQRT; x++) {
				goal[v].x = x;
				goal[v].y = y;
				v++;
			}
		}
	}

	/**
	 * array with one element for each position on the board. element (x,y) on
	 * the board is (NSQRT * y) + x in this array ideally this would be
	 * byte[NSQRT][NSQRT], but this makes creating a new board with the copy
	 * constructor (which we do _a_lot_) too expensive.
	 */
	private byte[] board;

	private int distance;

	private int bound;

	private int blankX, blankY;

	private int prevDx, prevDy;

	private int depth;

	/**
	 * create a board by deterministically shuffling the puzzle a given number
	 * of times.
	 */
	public Board(int length) {
		board = new byte[NSQRT * NSQRT];

		// Generate a starting position by shuffling the blanc around
		// in cycles. Just cycling along the outer bounds of the
		// board yields low quality start positions whose solutions
		// require less than 'length' steps. Therefore we use two
		// alternating cycling dimensions.

		// start with "solution"
		for (int i = 0; i < board.length; i++) {
			board[i] = (byte) i;
		}
		blankX = 0;
		blankY = 0;

		// size of cycle. alternates between NSQRT and (NSQRT - 1)
		int n = NSQRT - 1;

		for (int i = 0; i < length; i++) {
			// direction we should go
			int dx;
			int dy;

			if (blankX == 0 && blankY == 0) {
				// at starting position, change cycle dimension
				if (n == NSQRT) {
					n = NSQRT - 1;
				} else {
					n = NSQRT;
				}
			}

			if (blankX == 0 && blankY < n - 1) { // going down
				dx = 0;
				dy = 1;
			} else if (blankY == n - 1 && blankX < n - 1) { // going to the
				// right
				dx = 1;
				dy = 0;
			} else if (blankX == n - 1 && blankY > 0) { // going up
				dx = 0;
				dy = -1;
			} else if (blankY == 0 && blankX > 0) { // going left
				dx = -1;
				dy = 0;
			} else {
				throw new Error("not going in any direction");
			}
			move(dx, dy);
		}

		// reset values changed by calls to move()
		bound = 0;
		prevDx = 0;
		prevDy = 0;
		depth = 0;
		distance = calculateBoardDistance();
	}

	/**
	 * Create a new board. Read initial board position form a file. File should
	 * contain one character per position, denoting the value of each position
	 * as a hexadecimal number.
	 * 
	 * @throws Exception
	 * @throws IOException
	 */
	public Board(String fileName) throws Exception {
		board = new byte[NSQRT * NSQRT];
		bound = 0;
		prevDx = 0;
		prevDy = 0;
		depth = 0;
		blankX = -1;
		blankY = -1;

		Scanner scanner = new Scanner(new FileInputStream(fileName));
		for (int i = 0; i < board.length; i++) {
			String token = null;
			try {
				token = scanner.next();
			} catch (NoSuchElementException e) {
				throw new Exception(String.format("File \"%s\" has an invalid format", fileName));
			}
			int position;
			if (token.equals("..")) {
				position = 0;
			} else {
				try {
					position = Integer.parseInt(token);
					if (position <= 0 || position > NPUZZLE) throw new NumberFormatException();
				} catch (NumberFormatException e) {
					throw new Exception(String.format("Invalid tile \"%s\" in file \"%s\"", token, fileName));
				}
			}
			if (position == 0) {
				if (blankX != -1) {
					throw new Exception(String.format("Multiple blank tiles in file \"%s\"", fileName));
				}
				blankX = i % NSQRT;
				blankY = i / NSQRT;
			}
			board[i] = (byte) position;
		}
		if (blankX == -1) {
			throw new Exception(String.format("No blank tile in file \"%s\"", fileName));
		}
		byte[] boardClone = new byte[board.length];
		System.arraycopy(board, 0, boardClone, 0, board.length);
		Arrays.sort(boardClone);
		for (int i = 0; i < boardClone.length; ++i) {
			if (boardClone[i] != i) {
				throw new Exception(String.format("File \"%s\" has invalid tiles", fileName));
			}
		}
		distance = calculateBoardDistance();
	}

	public void init(Board original) {
		System.arraycopy(original.board, 0, board, 0, NSQRT * NSQRT);

		distance = original.distance;
		bound = original.bound;
		blankX = original.blankX;
		blankY = original.blankY;
		prevDx = original.prevDx;
		prevDy = original.prevDy;
		depth = original.depth;
	}

	/**
	 * Copy constructor
	 */
	public Board(Board original) {
		board = new byte[NSQRT * NSQRT];

		init(original);
	}

	/**
	 * Makes depth equal 0. Needed for job owner
	 */
	public void dropDepth(){
		depth = 0;
	}

	/**
	 * get value of tile at given position
	 */
	private byte getBoardValue(int x, int y) {
		return board[(NSQRT * y) + x];
	}

	/**
	 * set value of tile at given position
	 */
	private void setBoardValue(byte v, int x, int y) {
		board[(NSQRT * y) + x] = v;
	}

	/**
	 * Manhattan distance of the given coordinate to the goal position of the
	 * given value.
	 */
	private int tileDistance(int v, int x, int y) {
		if (v == 0) {
			// blank always in the right position
			return 0;
		}

		return Math.abs(goal[v].x - x) + Math.abs(goal[v].y - y);
	}

	/**
	 * Calculates the total distance of all elements of this puzzle to the goal.
	 * 
	 */
	private int calculateBoardDistance() {
		int result = 0;
		for (int y = 0; y < NSQRT; y++) {
			for (int x = 0; x < NSQRT; x++) {
				result += tileDistance(getBoardValue(x, y), x, y);
			}
		}
		return result;
	}

	/**
	 * Moves the blank in the given direction. Also updates bound, distance and
	 * depth.
	 */
	private void move(int dx, int dy) {
		int x = blankX + dx;
		int y = blankY + dy;
		byte v = getBoardValue(x, y);

		bound--;
		distance += -tileDistance(v, x, y) + tileDistance(v, blankX, blankY);
		depth++;

		setBoardValue((byte) 0, x, y);
		setBoardValue(v, blankX, blankY);

		prevDx = dx;
		prevDy = dy;
		blankX = x;
		blankY = y;
	}

	/**
	 * Make all possible moves with this board position. As an optimization,
	 * does not "undo" the move which created this board. Elements in the
	 * returned array may be "null".
	 */
	public Board[] makeMoves() {
		Board[] result = new Board[BRANCH_FACTOR];
		int n = 0;

		if (blankX > 0 && prevDx != 1) {
			result[n] = new Board(this);
			result[n].move(-1, 0);
			n++;
		}

		if (blankX < (NSQRT - 1) && prevDx != -1) {
			result[n] = new Board(this);
			result[n].move(1, 0);
			n++;
		}

		if (blankY > 0 && prevDy != 1) {
			result[n] = new Board(this);
			result[n].move(0, -1);
			n++;
		}

		if (blankY < (NSQRT - 1) && prevDy != -1) {
			result[n] = new Board(this);
			result[n].move(0, 1);
			n++;
		}
		return result;
	}

	/**
	 * Make all possible moves with this board position. As an optimization,
	 * does not "undo" the move which created this board. Elements in the
	 * returned array may be "null".
	 */
	public Board[] makeMoves(BoardCache cache) {
		Board[] result = new Board[BRANCH_FACTOR];
		int n = 0;

		if (blankX > 0 && prevDx != 1) {
			// result[n] = new Board(this);
			result[n] = cache.get(this);
			result[n].move(-1, 0);
			n++;
		}

		if (blankX < (NSQRT - 1) && prevDx != -1) {
			// result[n] = new Board(this);
			result[n] = cache.get(this);
			result[n].move(1, 0);
			n++;
		}

		if (blankY > 0 && prevDy != 1) {
			// result[n] = new Board(this);
			result[n] = cache.get(this);
			result[n].move(0, -1);
			n++;
		}

		if (blankY < (NSQRT - 1) && prevDy != -1) {
			// result[n] = new Board(this);
			result[n] = cache.get(this);
			result[n].move(0, 1);
			n++;
		}

		return result;
	}

	/**
	 * Manhattan distance of this board to the solution of the 24 puzzle
	 */
	public int distance() {
		return distance;
	}

	/**
	 * Returns the depth of this board. A board created my making a move with a
	 * board of depth N has a depth of N+1.
	 */
	public int depth() {
		return depth;
	}

	/**
	 * returns the bound of this board.
	 */
	public int bound() {
		return bound;
	}

	/**
	 * sets the bound of this board.
	 */
	public void setBound(int bound) {
		if (depth != 0) {
			System.err.println("warning: setting bound only makes sense at"
					+ "the initial job");
		}
		this.bound = bound;
	}

	/**
	 * returns string representing board position.
	 */
	public String toString() {
		String result = "";
		for (int y = 0; y < NSQRT; y++) {
			for (int x = 0; x < NSQRT; x++) {
				byte value = getBoardValue(x, y);

				if (value == 0) {
					result += " .. ";
				} else {
					result += String.format(" %2d ", value);
				}
			}
			result += "\n";
		}
		return result;
	}
}
