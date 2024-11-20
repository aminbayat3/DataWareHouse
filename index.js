const fs = require("fs");
const path = require("path");
const { Client } = require("pg");

const client = new Client({
  user: "postgres",
  host: "localhost",
  database: "aau_dwh",
  password: "AmenTest",
  port: 5432,
});

const normalizeGrade = (grade) => {
  if (grade === "not graded" || grade === "not taken") return null;
  if (grade === "failed") return 5;
  return parseInt(grade) || null;
};

const parseLecturerName = (name) => {
  const titleSet = new Set([
    "Dipl.-Ing.",
    "DI.",
    "Dr.",
    "Mag.",
    "B.Sc.",
    "M.Sc.",
  ]);
  const parts = name.trim().split(" ");

  let rank = [];
  let titles = [];
  let fullName = [];
  let foundTitle = false;

  for (const part of parts) {
    if (titleSet.has(part)) {
      titles.push(part);
      foundTitle = true;
    } else if (!foundTitle) {
      rank.push(part);
    } else {
      fullName.push(part);
    }
  }

  return {
    rank: rank.join(" ").trim(),
    titles: titles.join(", ").trim(),
    name: fullName.join(" ").trim(),
  };
};

const ExerciseOne = async () => {
  await client.connect();
  try {
    await client.query("BEGIN");

    const metadata = JSON.parse(
      fs.readFileSync("./aau_data/aau_metadata.json", "utf8")
    );

    for (const studyPlan of [
      ...metadata.bachelor_study_plans,
      ...metadata.master_study_plans,
    ]) {
      await client.query(
        `INSERT INTO aau_dwh.StudyPlan (StudyPlanID, Title, Degree, Branch) 
               VALUES ($1, $2, $3, $4) 
               ON CONFLICT (StudyPlanID) DO NOTHING`,
        [
          studyPlan.id,
          studyPlan.name,
          studyPlan.type || "Bachelor",
          studyPlan.branch,
        ]
      );
    }

    for (const lecturer of metadata.lecturers) {
      const { rank, titles, name } = parseLecturerName(lecturer.name);

      await client.query(
        `INSERT INTO aau_dwh.Lecturer (LecturerID, Name, Rank, Title, Department) 
               VALUES ($1, $2, $3, $4, $5) 
               ON CONFLICT (LecturerID) DO NOTHING`,
        [lecturer.id, name, rank, titles, lecturer.department]
      );
    }

    const courses = JSON.parse(
      fs.readFileSync("./aau_data/aau_corses.json", "utf8")
    );
    for (const level of ["bachelor", "master"]) {
      for (const course of courses[level]) {
        await client.query(
          `INSERT INTO aau_dwh.Course (CourseID, Title, Type, ECTS, Department, University) 
                   VALUES ($1, $2, $3, $4, $5, $6) 
                   ON CONFLICT (CourseID) DO NOTHING`,
          [
            course.id,
            course.title,
            course.type,
            parseInt(course.ECTS),
            course.department,
            metadata.name,
          ]
        );
      }
    }

    const resultsDir = "./aau_data/results/";
    const resultFiles = fs.readdirSync(resultsDir);

    for (const file of resultFiles) {
      const resultData = JSON.parse(
        fs.readFileSync(path.join(resultsDir, file), "utf8")
      );

      let examDate = resultData.date;

      if (!examDate.includes("-")) {
        examDate = `${examDate}-01-01`;
      }

      const [year, month, day] = examDate.split("-").map(Number);
      const semester = month <= 6 ? "Summer" : "Winter";

      const timeRes = await client.query(
        `INSERT INTO aau_dwh.Time (ExamDate, Day, Month, Semester, Year) 
               VALUES ($1, $2, $3, $4, $5) 
               ON CONFLICT (ExamDate) DO NOTHING 
               RETURNING TimeID`,
        [examDate, day, month, semester, year]
      );
      const timeID = timeRes.rows[0]?.timeid;

      for (const result of resultData.results) {
        await client.query(
          `INSERT INTO aau_dwh.Student (Matno, Name) 
                   VALUES ($1, $2) 
                   ON CONFLICT (Matno) DO NOTHING`,
          [result.matno, result.name]
        );

        await client.query(
          `INSERT INTO aau_dwh.Grades (StudentID, CourseID, LecturerID, StudyPlanID, TimeID, Grade) 
                   VALUES ($1, $2, $3, $4, $5, $6)`,
          [
            result.matno,
            resultData.course,
            resultData.examinator,
            result.studyplan,
            timeID,
            normalizeGrade(result.grade),
          ]
        );
      }
    }

    await client.query("COMMIT");
    console.log("Data loading completed successfully.");
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("Transaction failed. Rolled back changes.", err.message);
  } finally {
    await client.end();
  }
};

const ExerciseTwo = async () => {
    await client.connect();
  
    const res = await client.query(`SELECT DISTINCT lecturerID FROM aau_dwh.Grades;`);
    const lecturerIds = res.rows.map(row => row.lecturerid);
  
    const caseClauses = lecturerIds.map(
      lecturerID => `
        ROUND(AVG(CASE WHEN lecturerID = '${lecturerID}' THEN grade END), 2) AS "${lecturerID}_AvgGrade"`
    );
  
    const query = `
      SELECT
        studentID,
        ${caseClauses.join(",\n      ")}
      FROM
        aau_dwh.Grades
      GROUP BY
        studentID;
    `;

    // alternative query 
  //   const query = `
  //   SELECT
  //     studentID,
  //     lecturerID,
  //     ROUND(AVG(grade), 2)
  //   FROM
  //     aau_dwh.Grades
  //   GROUP BY
  //     studentID,
  //     lecturerID
  // `;
  
    console.log("Executing Query: \n", query); 

    try {
        const result = await client.query(query);
        console.log("Query Results:");
        console.table(result.rows); 
    } catch (err) {
        console.error("Error executing query:", err.message);
    }

    await client.end();
}

// await ExerciseOne().catch((err) => console.error("Error:", err.message));

console.log("***********************************************************************************");

ExerciseTwo().catch(err => console.error("Error:", err.message));
