package com.example.kafkaasyncdemo.application.port.secondary.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

import java.util.Objects;

import static jakarta.persistence.GenerationType.IDENTITY;

@Entity(name = "score_details")
public class ScoreDetailsEntity {

    @Id
    @GeneratedValue(strategy = IDENTITY)
    private Long id;

    @Column(name = "student_id")
    private String studentId;

    @Column(name = "subject")
    private String subject;

    @Column(name = "avg_score")
    private Double avgScore;

    public ScoreDetailsEntity() {
    }

    public ScoreDetailsEntity(String studentId, String subject, Double avgScore) {
        this.studentId = studentId;
        this.subject = subject;
        this.avgScore = avgScore;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getStudentId() {
        return studentId;
    }

    public void setStudentId(String studentId) {
        this.studentId = studentId;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public Double getAvgScore() {
        return avgScore;
    }

    public void setAvgScore(Double avgScore) {
        this.avgScore = avgScore;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ScoreDetailsEntity that = (ScoreDetailsEntity) o;
        return Objects.equals(id, that.id) && Objects.equals(studentId, that.studentId) && Objects.equals(subject, that.subject) && Objects.equals(avgScore, that.avgScore);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, studentId, subject, avgScore);
    }

    @Override
    public String toString() {
        return "ScoreDetailsEntity{" +
                "id=" + id +
                ", studentId='" + studentId + '\'' +
                ", subject='" + subject + '\'' +
                ", avgScore=" + avgScore +
                '}';
    }
}
